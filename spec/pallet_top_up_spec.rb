require 'rails_helper'
require 'rspec'
require 'spec_helper.rb'

class TxnHelper
  def initialize(&block)
    @fiber ||= Fiber.new { block.call }
  end

  def start
    @fiber.resume
  end

  def resume
    @fiber.resume
  end
end

class DecrementingTxn < TxnHelper
  def initialize(pallet_id)
    update = Proc.new do
      with_fresh_connection do |ctx|
        ctx.execute <<~SQL
          BEGIN;
        SQL

        ctx.execute <<~SQL
          SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
        SQL

        capacity = ctx.execute(<<~SQL)
          SELECT capacity FROM pallets WHERE id = #{pallet_id}
        SQL
          .then { |result| result.to_a.first["capacity"] }

        if capacity > 0
          ctx.execute <<~SQL
            UPDATE pallets SET capacity = capacity - 1 WHERE id = #{pallet_id}
          SQL
        end

        ctx.execute <<~SQL
          COMMIT;
        SQL
      end
    end

    super(&update)
  end
end

class IncrementingTxn < TxnHelper
  def initialize(pallet_id, by: 1)
    update = Proc.new do
      with_fresh_connection do |ctx|
        ctx.execute <<~SQL
          BEGIN;
        SQL

        ctx.execute <<~SQL
          SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
        SQL

        capacity = ctx.execute(<<~SQL)
          SELECT capacity FROM pallets WHERE id = #{pallet_id}
        SQL
          .then { |result| result.to_a.first["capacity"] }

        if capacity > 0
          ctx.execute <<~SQL
            UPDATE pallets SET capacity = #{capacity} - #{by} WHERE id = #{pallet_id}
          SQL
        end

        ctx.execute <<~SQL
          COMMIT;
        SQL
      end
    end

    super(&update)
  end
end

def peek_on_capacity(pallet_id)
  TxnHelper.new do
    with_fresh_connection do |ctx|
      ctx.execute <<~SQL
        BEGIN;
        SET statement_timeout = '1s';
      SQL

      ctx.execute <<~SQL
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
      SQL

      puts "about to search for capacity"
      capacity = ctx.execute(<<~SQL)
        SELECT capacity FROM pallets WHERE id = #{pallet_id}
      SQL
        .then { |result| result.to_a.first["capacity"] }

      puts "user txn found capacity: #{capacity}"

      sleep 1
      ctx.execute <<~SQL
          COMMIT;
      SQL
    end
  end
end

describe 'PalletTopUp' do
  let(:connection_pool) { ActiveRecord::Base.connection_pool }
  # Now... have something that gets a pallet. Checks if it has capacity, then inserts an item.
  # -- Demonstrate how this works with diff transaction isolation levels

  # Read committed. Show that for something that takes snapshots:
  # -- you can get an inconsistent view of the database.
  # --- Perhaps the pallet's contents are loaded, counted, then they're loaded in a different place and listed. Show count and list get different results.

  # Repeatable read. Show that
  # -- you get a consistent view
  # --- show example from above
  # -- you will get a serialization error if you write something that's changed since transaction start
  # -- you can still insert records in such a way that makes the database inconsistent.

  # Serializable. Show that
  # -- you can't get into the bad case you were trying before.
  # --- Explain that serializable is just a slightly stricter repeatable read

  # Side idea: pallet has a "vacancy number" the app tries to update

  context 'Read Committed' do

    it 'can overfill pallets' do
      # Using this Txn isolation level
      # Be explicit about what lines cannot be moved/altered.
      pallet = Pallet.create(capacity: 1)
      pallet_id = pallet.id

      interfering_txn = DecrementingTxn.new(pallet_id)

      with_fresh_connection do |ctx|
        ctx.execute <<~SQL
          BEGIN;
        SQL

        ctx.execute <<~SQL
          SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
        SQL

        capacity = ctx.execute(<<~SQL)
          SELECT capacity FROM pallets WHERE id = #{pallet_id}
        SQL
          .then { |result| result.to_a.first["capacity"] }

        interfering_txn.run_to_end

        if capacity > 0
          ctx.execute <<~SQL
            UPDATE pallets SET capacity = capacity - 1 WHERE id = #{pallet_id}
          SQL
        end

        ctx.execute <<~SQL
          COMMIT;
        SQL
      end

      # Why didn't the interfearing transaction wait? The main transaction had already selected the row!
      # Lesson: Read Committed transaction isolation pretty much just makes sure your writes are atomic, they
      # guarantee no consistent view of the database between select statements.

      # Change the code such that this expectation passes.
      # expect(pallet.reload.capacity).to eq(0)
    end

    # Done.
    it 'can lose updates' do
      pallet = Pallet.create(capacity: 0)
      pallet_id = pallet.id

      interfering_txn = IncrementingTxn.new(pallet_id, by: 20)

      with_fresh_connection do |ctx|
        ctx.execute <<~SQL
          BEGIN;
        SQL

        ctx.execute <<~SQL
          SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
        SQL

        capacity = ctx.execute(<<~SQL)
          SELECT capacity FROM pallets WHERE id = #{pallet_id}
        SQL
          .then { |result| result.to_a.first["capacity"] }

        interfering_txn.run_to_end

        ctx.execute <<~SQL
          UPDATE pallets SET capacity = #{capacity} + 20 WHERE id = #{pallet_id}
        SQL

        ctx.execute <<~SQL
          COMMIT;
        SQL
      end

      # Why was the update lost here?

      # Change the code such that this expectation passes.
      # expect(pallet.reload.capacity).to eq(40)
    end

    it 'the order of query arrival can greatly influence execution time' do
      # What's the useful bit of this example anyway? What do I want to teach?
      # -> That lock timeouts are good. That our application doesn't use them. That your query can hang and time out
      # other's web request.
      #
      # Make this example such that: The order in which queries arrive demonstrates big differences in completion time.
      pallet = Pallet.create(capacity: 0)
      pallet_id = pallet.id
      txn1, txn2 = [peek_on_capacity(pallet_id)] * 2

      with_fresh_connection do |ctx|
        ctx.execute <<~SQL
          BEGIN;
        SQL

        ctx.execute <<~SQL
          SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
        SQL


        txn1.start
        sleep 1

        ctx.execute(<<~SQL)
          ALTER TABLE pallets ADD COLUMN zorph INT;
        SQL
        puts "LOCKED and altering table!"
        # While it's hard at work...
        txn2.start
        sleep 2

        puts "about to rollback"
        ctx.execute <<~SQL
          ROLLBACK;
        SQL
      end

      txn2.join
      txn1.join
      # Why does the ALTER TABLE make txn2 wait? It's only querying a field that the ALTER doesn't touch.
    end

    context 'with explicit locking' do
      # https://www.postgresql.org/docs/11/explicit-locking.html

      it 'can take out a lock at select time' do
        # https://www.postgresql.org/docs/11/sql-select.html#SQL-FOR-UPDATE-SHARE
        pallet = Pallet.create(capacity: 1)
        pallet_id = pallet.id

        interfering_txn = Proc.new do
          Thread.new do
            with_fresh_connection do |ctx|
              ctx.execute <<~SQL
                BEGIN;
              SQL

              ctx.execute <<~SQL
                SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
              SQL

              capacity = ctx.execute(<<~SQL)
                SELECT capacity FROM pallets WHERE id = #{pallet_id}
              SQL
                .then { |result| result.to_a.first["capacity"] }

              if capacity > 0
                ctx.execute <<~SQL
                  UPDATE pallets SET capacity = capacity - 1 WHERE id = #{pallet_id}
                SQL
              end
              puts "interfearer: finished updating"

              ctx.execute <<~SQL
                COMMIT;
              SQL
            end
          end
        end

        main_txn = Fiber.new do
          with_fresh_connection do |ctx|
            ctx.execute <<~SQL
              BEGIN;
            SQL

            ctx.execute <<~SQL
              SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
            SQL

            capacity = ctx.execute(<<~SQL)
              SELECT capacity FROM pallets WHERE id = #{pallet_id} FOR UPDATE
            SQL
              .then { |result| result.to_a.first["capacity"] }

            thread = interfering_txn.call
            sleep 3

            ctx.execute <<~SQL
              UPDATE pallets SET capacity = capacity - 1 WHERE id = #{pallet_id}
            SQL
            puts "main txn: finished updating"

            ctx.execute <<~SQL
              COMMIT;
            SQL
            thread.join
          end
        end

        main_txn.resume

        # Why does this spec fail right now? We select FOR UPDATE, shouldn't that prevent the interfearing
        # transaction from reading them?

        expect(pallet.reload.capacity).to eq(0)
      end
    end
  end

  context 'Repeatable Read' do

    it 'fails due to a concurrent update' do
      Pallet.create!(capacity: 1)
      interfering_txn = Fiber.new do
        with_fresh_connection do |ctx|
          ctx.execute <<~SQL
            BEGIN;
          SQL

          ctx.execute <<~SQL
            SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
          SQL

          # Acquire a lock (which?) on the only pallet
          ctx.execute <<~SQL
            UPDATE pallets SET capacity = capacity + 1 WHERE capacity = 1;
          SQL

          ctx.execute <<~SQL
            COMMIT;
          SQL
        end
      end

      main_txn = Fiber.new do
        with_fresh_connection do |ctx|
          ctx.execute <<~SQL
            BEGIN; SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
          SQL

          #  First non-transaction control statement triggers the snapshot.
          ctx.execute <<~SQL
            SELECT 0;
          SQL

          # Other transaction commits an update to a row we are about to update ourselves.
          interfering_txn.resume

          # First difference from read committed.
          ctx.execute <<~SQL
            UPDATE pallets SET capacity = capacity + 1;
          SQL

          ctx.execute <<~SQL
            COMMIT;
          SQL

        end
      end

      expect { main_txn.resume }.to raise_exception(ActiveRecord::SerializationFailure)
    end

    it 'it cannot see inserts from other transactions'
  end

  context 'Serializable' do
    it 'aborts transactions that didnt see a perfect view of the database'
  end
end

def connection_pool
  ActiveRecord::Base.connection_pool;
end

def with_fresh_connection
  yield ctx = connection_pool.checkout;
ensure
  connection_pool.checkin(ctx)
end

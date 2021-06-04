require 'rails_helper'
require 'rspec'
require 'spec_helper.rb'

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
      pallet = Pallet.create(capacity: 1)
      pallet_id = pallet.id

      interfering_txn = Fiber.new do
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

      main_txn = Fiber.new do
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

          interfering_txn.resume

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

      main_txn.resume

      # Why didn't the interfearing transaction wait? The main transaction had already selected the row!
      # Lesson: Read Committed transaction isolation pretty much just makes sure your writes are atomic, they
      # guarantee no consistent view of the database between select statements.

      # Change the code such that this expectation passes. (changing the expectation is automatic fail in case that's
      # not clear)
      expect(pallet.reload.capacity).to eq(0)
    end

    it 'can use stale data to make decisions' do
      #  Show that it can read a value inside a transaction, have that value changed and then write that value
    end
  end

  def connection_pool; ActiveRecord::Base.connection_pool; end
  def with_fresh_connection; yield ctx = connection_pool.checkout; ensure connection_pool.checkin(ctx) end

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
require 'rails_helper'
require 'rspec'
require 'spec_helper.rb'

describe 'PalletTopUp' do
  before do
    # Do nothing
  end

  after do
    # Do nothing
  end

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
    before do
      Pallet.connection.execute(<<~SQL)
        SET TRANSACTION ISOLATION LEVEL READ COMMITTED
      SQL
    end

    it 'can overfill pallets' do
      pallet = Pallet.create(capacity: 10)
      9.times { |i| pallet.items.build(code: "item ##{i}") }
    end

    it 'can use stale data to make decisions' do
      #  Show that it can read a value inside a transaction, have that value changed and then write that value
    end
  end

  context 'Repeatable Read' do
    it 'prevents you from seeing an inconsistent view of the database' do
      # it querys some value. another transaction changes that value.
    end

    it 'it cannot see inserts from other transactions'
  end

  context 'Serializable' do
    it 'aborts transactions that didnt see a perfect view of the database'
  end
end
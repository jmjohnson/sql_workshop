class CreatePallets < ActiveRecord::Migration[6.0]
  def change
    create_table :pallets do |t|
      t.integer :capacity
    end

    100.times do
      Pallet.create(capacity: 10)

    end
  end
end

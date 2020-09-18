class CreateItems < ActiveRecord::Migration[6.0]
  def change
    create_table :items do |t|
      t.string :name
      t.integer :pallet_id
      t.string :code
    end


    Pallet.all.in_batches.each_record do |pallet|
      pallet.items = [Item.new(code: SecureRandom.uuid[0..7])] * 9
    end
  end
end

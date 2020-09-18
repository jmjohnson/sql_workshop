class AddSkuModel < ActiveRecord::Migration[6.0]
  def change
    create_table :skus do |t|
      t.string :code
    end
  end
end


class Pallet < ApplicationRecord
  has_many :items, autosave: true

end
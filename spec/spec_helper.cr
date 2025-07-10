require "spec"

def be_within(delta, of value)
  be_close value, delta: delta
end

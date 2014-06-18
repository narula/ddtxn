package ddtxn

func CKey(x uint64, ch rune) Key {
	var b [16]byte
	var i uint64
	for i = 0; i < 8; i++ {
		b[i] = byte((x >> (i * 8)))
	}
	b[8] = byte(ch)
	return Key(b)
}

func UndoCKey(k Key) (uint64, rune) {
	b := [16]byte(k)
	var x uint64
	var i uint64
	for i = 0; i < 8; i++ {
		v := uint32(b[i])
		x = x + uint64(v<<(i*8))
	}
	return x, rune(b[8])
}

func TKey(x uint64, y uint64) Key {
	var b [16]byte
	var i uint64
	for i = 0; i < 8; i++ {
		b[i] = byte((x >> (i * 8)))
	}
	for i = 8; i < 16; i++ {
		b[i] = byte((y >> (i * 8)))
	}
	return Key(b)
}

func SKey(s string) Key {
	var b [16]byte
	end := 16
	if len(s) < 16 {
		end = len(s)
	}
	for i := 0; i < end; i++ {
		b[i] = s[i]
	}
	return Key(b)
}

func UserKey(bidder int) Key {
	return CKey(uint64(bidder), 'u')
}

func BidKey(id uint64) Key {
	return CKey(id, 'b')
}

func PairKey(x uint32, y uint32, ch rune) Key {
	var b [16]byte
	var i uint64
	for i = 0; i < 4; i++ {
		b[i] = byte((x >> (i * 8)))
		b[i+4] = byte((y >> (i * 8)))
	}
	b[8] = byte(ch)
	return Key(b)
}

func PairBidKey(bidder uint64, product uint64) Key {
	return PairKey(uint32(bidder), uint32(product), 'z')
}

func ItemKey(item uint64) Key {
	return CKey(item, 'i')
}

func ProductKey(product int) Key {
	return CKey(uint64(product), 'p')
}

func MaxBidKey(item uint64) Key {
	return CKey(item, 'm')
}

func NumBidsKey(item uint64) Key {
	return CKey(item, 'n')
}

func BidsPerItemKey(item uint64) Key {
	return CKey(item, 'p')
}

func MaxBidBidderKey(item uint64) Key {
	return CKey(item, 'a')
}

func BuyNowKey(item uint64) Key {
	return CKey(item, 'n')
}

func CommentKey(item uint64) Key {
	return CKey(item, 'c')
}

func ItemsByCatKey(item uint64) Key {
	return CKey(item, 't')
}

func ItemsByRegKey(region uint64, categ uint64) Key {
	return PairKey(uint32(region), uint32(categ), 'r')
}

func RatingKey(user uint64) Key {
	return CKey(user, 'c')
}

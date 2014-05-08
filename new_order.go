package ddtxn

type Customer struct {
	id uint64
}

type NewOrder struct {
	id uint64
}

type NItem struct {
	id uint64
}

type Stock struct {
	id uint64
}

type OrderLine struct {
	id uint64
}

func (w *Worker) NewOrderTxn(customer, warehouse, district uint64) {
	// Get customer info
	// Get district info, next_o_id
	// insert new order
	// Update district's next_o_id++
	// insert into oorder (?)
	// getItem
	// getStock
	// updateStock  -->  s_ytd
	// insertOrderLine
	// for number of items:
	//    insert order
}

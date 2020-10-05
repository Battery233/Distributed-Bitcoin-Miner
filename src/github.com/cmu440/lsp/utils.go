package lsp

type unAckedMessage struct {
	message        *Message
	currentBackoff int
	epochCounter   int
}

func calculateCheckSum(id int, seq int, size int, payload []byte) uint16 {
	sum := Int2Checksum(id)
	sum += Int2Checksum(seq)
	sum += Int2Checksum(size)
	sum += ByteArray2Checksum(payload)
	//todo fix checksum
	return uint16(sum)
}

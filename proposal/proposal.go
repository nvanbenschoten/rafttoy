package proposal

import "encoding/binary"

// EncProposal represents a proposal with an associated id.
type EncProposal []byte

func Encode(b []byte) EncProposal {
	enc := make([]byte, 8+len(b))
	copy(enc[8:], b)
	return enc
}

func (enc EncProposal) SetID(id int64) {
	binary.LittleEndian.PutUint64(enc[:8], uint64(id))
}

func (enc EncProposal) GetID() int64 {
	return int64(binary.LittleEndian.Uint64(enc[:8]))
}

func (enc EncProposal) GetBytes() []byte {
	return enc[8:]
}

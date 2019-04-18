package proposal

import "encoding/binary"

// Proposal represents an in-memory Raft proposal.
type Proposal struct {
	ID  int64
	Key []byte
	Val []byte
}

// EncProposal represents an encoded Raft proposal.
//
// Encoding:
//  int64          : id
//  int64          : key_len
//  [key_len]bytes : key
//  int64          : val_len
//  [key_len]bytes : val
type EncProposal []byte

// SetID sets the encoded proposal's ID without decoding it.
func (enc EncProposal) SetID(id int64) {
	putInt64(enc[:8], id)
}

// GetID gets the encoded proposal's ID without decoding it.
func (enc EncProposal) GetID() int64 {
	return getInt64(enc[:8])
}

// Encode maps a Proposal to an EncProposal.
func Encode(p Proposal) EncProposal {
	enc := make([]byte, 3*8+len(p.Key)+len(p.Val))
	n := 0
	putInt64(enc[n:n+8], p.ID)
	n += 8
	putInt64(enc[n:n+8], int64(len(p.Key)))
	n += 8
	copy(enc[n:n+len(p.Key)], p.Key)
	n += len(p.Key)
	putInt64(enc[n:n+8], int64(len(p.Val)))
	n += 8
	copy(enc[n:n+len(p.Val)], p.Val)
	n += len(p.Val)
	if n != len(enc) {
		panic("accounting error")
	}
	return enc
}

// Decode maps an EncProposal to a Proposal.
func Decode(enc EncProposal) Proposal {
	var p Proposal
	n := 0
	p.ID = getInt64(enc[n : n+8])
	n += 8
	kl := int(getInt64(enc[n : n+8]))
	n += 8
	p.Key = enc[n : n+kl]
	n += kl
	vl := int(getInt64(enc[n : n+8]))
	n += 8
	p.Val = enc[n : n+vl]
	n += vl
	if n != len(enc) {
		panic("accounting error")
	}
	return p
}

func putInt64(b []byte, i int64) {
	binary.LittleEndian.PutUint64(b[:8], uint64(i))
}

func getInt64(b []byte) int64 {
	return int64(binary.LittleEndian.Uint64(b[:8]))
}

package v

import "encoding/binary"

const (
    magicNumber byte = 0x07
)

func MagicNumber() byte {
    return magicNumber
}

/* message:
 * ----------------------------------------------------------------------------------------------------
 * |<header>|<2 bytes length of service name>|<service name>|<4 bytes length of payload>|<payload>|
 * ----------------------------------------------------------------------------------------------------
 * header:
 *
 * [0x07]            [0x01]        [0x00000001]  [0x00000001]
 *   |                 |                |             |
 * (1B)magicNumber  (1B)version   (4B)cmd id      (4B)seq
 * -------------------------------------------------------------
 */

type Header [10]byte
type Message struct {
    *Header
    ServiceName string
    Payload []byte
}

func NewMessage(service string) *Message {
    header := Header{}
    header[0] = magicNumber

    return &Message{
        Header:&header,
        ServiceName:service,
    }
}

func (h Header) CheckMagicNumber() bool {
    return h[0] == magicNumber
}

func (h Header) Version() byte {
    return h[1]
}

func (h *Header) SetVersion(v byte) {
    h[1] = v
}

func (h Header) Cmd() uint32 {
    return binary.LittleEndian.Uint32(h[2:6])
}

func (h *Header) SetCmd(cmd uint32) {
    binary.LittleEndian.PutUint32(h[2:6], cmd)
}

func (h Header) Seq() uint32 {
    return binary.LittleEndian.Uint32(h[6:])
}

func (h *Header) SetSeq(seq uint32) {
    binary.LittleEndian.PutUint32(h[6:], seq)
}
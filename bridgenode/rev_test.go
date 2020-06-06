package bridgenode

import (
	"fmt"
	"os"
	"testing"
)

func TestRevDeserialize(t *testing.T) {
	var rb RevBlock
	rFile, err := os.Open("/home/calvin/.bitcoin/testnet3/testing1/rev00000.dat")
	if err != nil {
		t.Fatal(err)
	}
	rFile.Seek(int64(239695), 0)
	rb.Deserialize(rFile)
	fmt.Println(rb)
	fmt.Printf("txs: %d\n", len(rb.Txs))
}

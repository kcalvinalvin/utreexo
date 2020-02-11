package ibdsim

import (
	"fmt"
	"os"

	"github.com/mit-dci/utreexo/cmd/simutil"
	"github.com/mit-dci/utreexo/utreexo"
)

// ResumeGenProofs restores the variables from disk to memory.
// Returns the height, currentoffsetheight, and pOffset genproofs left at.
func ResumeGenProofs(
	isTestnet bool, offsetfinished chan bool) (int32, int32, uint32) {

	height := restoreHeight()
	currentoffsetheight := restoreCurrentOffset()
	pOffset := restoreProofFileOffset()

	return height, currentoffsetheight, pOffset
}

// restoreHeight restores height from simutil.HeightFilePath
func restoreHeight() int32 {

	var height int32

	// if there is a heightfile, get the height from that
	// heightFile saves the last block that was written to ttldb
	if simutil.HasAccess(simutil.HeightFilePath) {
		heightFile, err := os.OpenFile(
			simutil.HeightFilePath, os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			panic(err)
		}
		var t [4]byte
		_, err = heightFile.Read(t[:])
		if err != nil {
			panic(err)
		}
		height = simutil.BtI32(t[:])
	}
	return height
}

// restoreCurrentOffset restores the currentOffsetHeight
// from simutil.CurrentOffsetFilePath
func restoreCurrentOffset() int32 {

	var currentOffsetHeight int32

	// grab the last block height from currentoffsetheight
	// currentoffsetheight saves the last height from the offsetfile
	var currentOffsetHeightByte [4]byte

	currentOffsetHeightFile, err := os.OpenFile(
		simutil.CurrentOffsetFilePath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		panic(err)
	}
	_, err = currentOffsetHeightFile.Read(currentOffsetHeightByte[:])
	if err != nil {
		panic(err)
	}

	currentOffsetHeightFile.Read(currentOffsetHeightByte[:])
	currentOffsetHeight = simutil.BtI32(currentOffsetHeightByte[:])

	return currentOffsetHeight
}

// restoreProofFileOffset restores Poffset from simutil.POffsetCurrentOffsetFilePath
func restoreProofFileOffset() uint32 {

	// Gives the location of where a particular block height's proofs are
	// Basically an index
	var pOffset uint32

	if simutil.HasAccess(simutil.POffsetCurrentOffsetFilePath) {
		pOffsetCurrentOffsetFile, err := os.OpenFile(
			simutil.POffsetCurrentOffsetFilePath,
			os.O_CREATE|os.O_RDWR, 0600)
		if err != nil {
			panic(err)
		}
		pOffset, err = simutil.GetPOffsetNum(pOffsetCurrentOffsetFile)
		if err != nil {
			panic(err)
		}
		fmt.Println("Poffset restored to", pOffset)

	}
	return pOffset
}

// SaveGenproofs saves the state of genproofs so that when the
// user restarts, they'll be able to resume.
// Saves height, misc forest data, and pOffset
func saveGenproofs(
	forest *utreexo.Forest,
	pOffset uint32,
	height int32,
	heightFile *os.File,
	miscForestFile *os.File,
	pOffsetCurrentOffsetFile *os.File) error {

	// write to the heightfile
	_, err := heightFile.WriteAt(simutil.I32tB(height), 0)
	if err != nil {
		panic(err)
	}
	heightFile.Close()

	// write other misc forest data
	err = forest.WriteForest(miscForestFile)
	if err != nil {
		panic(err)
	}
	// write pOffset
	_, err = pOffsetCurrentOffsetFile.WriteAt(
		simutil.U32tB(pOffset), 0)
	if err != nil {
		panic(err)
	}

	return nil
}

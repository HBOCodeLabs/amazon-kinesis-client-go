package main

import (
	"encoding/base64"
	"fmt"
	"io"
	"io/ioutil"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"github.com/Clever/amazon-kinesis-client-go/kcl"
)

type sampleRecordProcessor struct {
	checkpointer   kcl.Checkpointer
	checkpointFreq time.Duration
	largestPair    kcl.SequencePair
	lastCheckpoint time.Time
	log            io.Writer
}

func newSampleRecordProcessor() *sampleRecordProcessor {
	return &sampleRecordProcessor{
		log:            ioutil.Discard,
		checkpointFreq: 60 * time.Second,
	}
}

func (srp *sampleRecordProcessor) Initialize(shardID string, checkpointer kcl.Checkpointer) error {
	// Use a file for all the logging, so that stdout remains dedicated to communication between this
	// process and the MLD.  The name of the file is unique to the PID and the shard ID being processed,
	// so that multiple processes started by the same MLD don't conflict.
	//
	// TODO: this obviously shouldn't be a hardcoded path, but it's enough for a prototype
	f, err := os.Create(filepath.Join("/tmp/kcl_stderr", fmt.Sprintf("%s-%d", shardID, os.Getpid())))
	if err != nil {
		panic(err)
	}
	srp.log = f

	// logging statements for debugging of the prototype
	fmt.Fprintf(srp.log, "initializing with shardID: %q and checkpointer: %#v\n", shardID, checkpointer)
	srp.lastCheckpoint = time.Now()
	fmt.Fprintf(srp.log, "last checkpoint: %s", srp.lastCheckpoint.Format(time.RFC3339))
	srp.checkpointer = checkpointer
	return nil
}

func (srp *sampleRecordProcessor) shouldUpdateSequence(pair kcl.SequencePair) bool {
	return srp.largestPair.IsLessThan(pair) || srp.largestPair.IsNil()
}

func (srp *sampleRecordProcessor) ProcessRecords(records []kcl.Record) error {
	for _, record := range records {
		seqNumber := new(big.Int)
		if _, ok := seqNumber.SetString(record.SequenceNumber, 10); !ok {
			fmt.Fprintf(os.Stderr, "could not parse sequence number '%s'\n", record.SequenceNumber)
			continue
		}
		pair := kcl.SequencePair{Sequence: seqNumber, SubSequence: record.SubSequenceNumber}
		if srp.shouldUpdateSequence(pair) {
			srp.largestPair = pair
		}

		// decode the actual data in the payload, and print it in the log, just for debugging purposes
		data, err := base64.StdEncoding.DecodeString(record.Data)
		if err != nil {
			panic(err)
		}
		fmt.Fprintf(srp.log, "record has data: %q\n", data)

	}

	// sync, to force any buffered data to be written to the underlying log file
	if f, ok := srp.log.(*os.File); ok {
		f.Sync()
	}

	// more debugging output
	fmt.Fprintf(srp.log, "time now: %s, last checkpoint: %s\n", time.Now().Format(time.RFC3339), srp.lastCheckpoint.Format(time.RFC3339))
	if time.Now().Sub(srp.lastCheckpoint) > srp.checkpointFreq {
		srp.checkpointer.Checkpoint(srp.largestPair)
		srp.lastCheckpoint = time.Now()
	}

	return nil
}

func (srp *sampleRecordProcessor) Shutdown(reason string) error {
	if reason == "TERMINATE" {
		fmt.Fprintf(os.Stderr, "Was told to terminate, will attempt to checkpoint.\n")
		srp.checkpointer.Shutdown()
	} else {
		fmt.Fprintf(os.Stderr, "Shutting down due to failover. Will not checkpoint.\n")
	}

	if srp.log != nil {
		if f, ok := srp.log.(*os.File); ok {
			fmt.Fprintf(srp.log, "Closing log file\n")
			f.Close()
		}
	}

	return nil
}

func main() {
	kclProcess := kcl.New(os.Stdin, os.Stdout, os.Stderr, newSampleRecordProcessor())
	kclProcess.Run()
}

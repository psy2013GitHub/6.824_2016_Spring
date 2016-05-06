package mapreduce

import (
	"io"
	"os"
	//"fmt"
	"encoding/json"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()

	// read output from map
	kv := KeyValue{"", ""}
	KeyValsMap := make(map[string] []string, 1024)
	reducde_fd_map := make(map[string] *json.Decoder, nMap)
	var dec *json.Decoder
	var ok bool
    for i:=0; i < nMap; i++ {
        reduce_fname := reduceName(jobName, i, reduceTaskNumber)
        dec, ok = reducde_fd_map[reduce_fname]
        if !ok {
        	fd, err := os.Open(reduce_fname)
        	if err != nil {
        		return
        	}
        	defer fd.Close()
        	dec = json.NewDecoder(fd)
        	//debug("doReduce decoder init: %s\n", dec)
        	reducde_fd_map[reduce_fname] = dec
        } else {
        	//debug("doReduce decoder ok: %s\n", dec)
        }
        for {
			err := dec.Decode(&kv)
			if err != nil {
				if err != io.EOF {
					debug("doReduce decoder.decode: %s\n", err)
					return
				}
				break
			}
			vals, ok := KeyValsMap[kv.Key]
			if !ok {
				vals := make([]string, 1)
				vals[0] = kv.Value
				KeyValsMap[kv.Key] = vals
			}
			vals = append(vals, kv.Value)
			KeyValsMap[kv.Key] = vals
		}
    }
    //debug("doReduce KeyValsMap:", KeyValsMap)

    // reduceF
    res := make(map[string] string)
    for k, v := range KeyValsMap {
    	res[k] = reduceF(k, v)
    }

    // write to merge file
    reduce_fname := mergeName(jobName, reduceTaskNumber)
    fd, err := os.Create(reduce_fname)
    if err != nil {
    	return
    }
    defer fd.Close()

    enc := json.NewEncoder(fd)
    for k, v := range res {
    	err := enc.Encode(&KeyValue{k, v})
    	if err != nil {
    		return
    	}
    }
}

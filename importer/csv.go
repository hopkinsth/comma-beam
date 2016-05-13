package importer

import "os"
import "io"
import "errors"
import "encoding/csv"


var InternalErr = errors.New("internal importer error, sorry :(")
var stopQueuing = errors.New("good idea or just codesmell")

type Config struct {
    fields []string `json:"fields"`
}

type Writer interface {
    WriteBatch([][]string) error
}

type Importer struct {
    fname string
    w Writer
    bch chan []string
    errs chan error
    BatchSize int
    MaxConcurrent int
}

func New(file string, w Writer) *Importer {
    return &Importer{
        file,
        w,
        make(chan []string),
        make(chan error),
        100,
        2,
    }
}

// imports your CSV data to wherever it needs to go
// will (should?) block until everything is all resolved
// or you got an error
func (i *Importer) Do() error {
    f, err := os.Open(i.fname)
    if err != nil {
        return err
    }
    
    for m := 0; m < i.MaxConcurrent; m += 1 {
        go i.queuer()
    }
    
    //cr := csv.NewReader(f)
    go i.reader(f)
    
    for err := range i.errs {
        if err != nil {
            return err
        }
    }
    
    return nil
}

func (i *Importer) reader(f *os.File) {
    cr := csv.NewReader(f)
    for {
        line, err := cr.Read()
        switch err {
            case nil:
                if len(line) > 0 {
                    // only queue if the line is not empty;
                    // we will use an empty line as a special thing
                    i.bch <- line
                }
            case io.EOF:
                // if we hit eof we need to stop all the writers
                for n := 0; n < i.MaxConcurrent; n += 1 {
                    i.bch <- nil
                }
                
                // close the err channel so anything waiting on it will stop
                close(i.errs)
                return
            default:
                // any other err, bail
                i.errs <- err
        }
    }
}

// queues a line in an internal buffer for writing later
func (i *Importer) queuer() {
    var qu [][]string
    for {
        select {
            case line := <-i.bch:
                if len(line) == 0 {
                    // empty line? assume we need to bail
                    // but flush
                    return
                }
                err := i.addQueue(line, &qu)
                if err != nil {
                    // dispatch errors to the error channel
                    i.errs <- err
                }
        }
    }
}

func (i *Importer) addQueue(line []string, qu *[][]string) error {
    if qu == nil {
        return InternalErr
    }
    if len(*qu) >= i.BatchSize {
        // write that data if the size of the queue
        // exceeds the batch size
        return i.w.WriteBatch(*qu)
    }
    
    // otherwise, just add it to the queue
    *qu = append(*qu, line)
    return nil
}
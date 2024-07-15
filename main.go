package main

import (
	"encoding/csv"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
)

type processRequest struct {
	InputFile        string
	OutputFile       string
	RecordsToProcess int
}

type compareRequest struct {
	Source      io.Reader
	Destination io.Writer
	// WriteHeader      bool
	// RecordsToSkip    int
	RecordsToProcess int
	// WordLimit        int
	// WorkerCount      int
}

type recordProcessRequest struct {
	YggdrasilID string
}

type recordProcessResponse struct {
	CleanedTitle string
}

const cleanedDataPath = "../ai_detect/data/content"

var errObjReadFailed = errors.New("failed to read content")

type xmlPost struct {
	Title string `xml:"title,attr"`
	Main  struct {
		Body string `xml:",innerxml"`
	} `xml:"main"`
}

type parseXMLResponse struct {
	Title string
}

func parseAndCleanXML(content string) (parseXMLResponse, error) {
	var doc xmlPost
	if err := xml.Unmarshal([]byte(content), &doc); err != nil {
		return parseXMLResponse{}, fmt.Errorf("unmarshal error: %w", err)
	}

	return parseXMLResponse{
		Title: doc.Title,
	}, nil
}

func getObject(name string) (string, error) {
	localPath := cleanedDataPath + "/" + name

	rawObj, err := os.ReadFile(localPath)

	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return "", errObjReadFailed
		}

		return "", err
	}

	return string(rawObj), nil
}

func processRecord(req recordProcessRequest) (recordProcessResponse, error) {
	cleanedFileName := req.YggdrasilID + ".xml"

	obj, err := getObject(cleanedFileName)
	if err != nil {
		return recordProcessResponse{}, fmt.Errorf("content get error: %w", err)
	}

	result, err := parseAndCleanXML(obj)
	if err != nil {
		return recordProcessResponse{}, fmt.Errorf("content cleaning error: %w", err)
	}

	return recordProcessResponse{
		CleanedTitle: result.Title,
	}, nil
}

func compareTitlesCSV(req compareRequest) error {
	csvReader := csv.NewReader(req.Source)
	_, err := csvReader.Read() // reading header line
	if err != nil {
		return fmt.Errorf("unable to parse file as CSV %w", err)
	}

	csvWriter := csv.NewWriter(req.Destination)
	defer csvWriter.Flush()

	if err := csvWriter.Write([]string{"yggdrasilId", "currentTitle", "trafilaturaTitle"}); err != nil {
		return fmt.Errorf("result output error %w", err)
	}

	var lineCount int

	for {
		record, err := csvReader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return fmt.Errorf("unable to parse file as CSV %w", err)
		}

		// record = [yggdrasilId,title,createdAt]
		procResp, err := processRecord(recordProcessRequest{
			YggdrasilID: record[0],
		})

		if err != nil {
			if errors.Is(err, errObjReadFailed) {
				log.Println("filer not found ", record[0])
				goto SKIPWRITE
			}
			return fmt.Errorf("unable to process line %d, %w", lineCount+1, err)
		}

		if err := csvWriter.Write([]string{
			record[0], record[1], procResp.CleanedTitle,
		}); err != nil {
			return fmt.Errorf("result output error line %d, %w", lineCount+1, err)
		}

	SKIPWRITE:

		lineCount++

		if req.RecordsToProcess > 0 && lineCount >= req.RecordsToProcess {
			break
		}
	}

	log.Printf("title comparison completed; lines processed: %d", lineCount)

	return nil
}

func compareTitles(req processRequest) error {
	log.Printf("title comparison started: %#v", req)

	source, err := os.Open(req.InputFile)
	if err != nil {
		return err
	}
	defer func() {
		if err := source.Close(); err != nil {
			log.Println("error closing file", err)
		}
	}()

	fl := os.O_CREATE | os.O_WRONLY | os.O_TRUNC

	destination, err := os.OpenFile(req.OutputFile, fl, 0666)
	if err != nil {
		return err
	}
	defer func() {
		if err := destination.Close(); err != nil {
			log.Println("error closing file", err)
		}
	}()

	return compareTitlesCSV(compareRequest{
		Source:           source,
		Destination:      destination,
		RecordsToProcess: req.RecordsToProcess,
	})
}

func main() {

	if err := compareTitles(processRequest{
		InputFile:        "data/query_result_2024-07-15T11_28_21.767505Z.csv",
		OutputFile:       "output.csv",
		RecordsToProcess: 0,
	}); err != nil {
		log.Fatal(err)
	}
}

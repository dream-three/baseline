package main

import (
    "bufio"
    "crypto/sha256"
    "encoding/csv"
    "encoding/hex"
    "fmt"
    "io"
    "math/rand"
    "net/http"
    "net/url"
    "os"
    "path/filepath"
    "sort"
    "strconv"
    "strings"
    "time"

    exifpkg "github.com/rwcarlsen/goexif/exif"
    "github.com/rwcarlsen/goexif/tiff"
    "github.com/otiai10/gosseract/v2"
)

const (
    baseURL        = "https://raw.githubusercontent.com/dream-three/baseline/refs/heads/main/"
    defaultMinutes = 60
    fetchPause     = 5 * time.Second
    logFile        = "shifts.log"
    maxDiffChanges = 10
    maxDiffChars   = 500
    zeroHash       = "0000000000000000000000000000000000000000000000000000000000000000"
)

var imageExts = []string{".jpg", ".jpeg", ".png", ".avif"}

func main() {
    rand.Seed(time.Now().UnixNano())

    baselineMode := promptBaselineMode()

    runningDir, err := os.Getwd()
    if err != nil {
        fmt.Printf("Error getting running directory: %v\n", err)
        return
    }

    if baselineMode {
        fetchBaselines(runningDir)
    }

    interval := promptInterval()

    for {
        runCycle(interval, runningDir)
        time.Sleep(time.Duration(interval) * time.Minute)
    }
}

func promptBaselineMode() bool {
    fmt.Print("Grab remote files first as baseline? (y/n): ")
    reader := bufio.NewReader(os.Stdin)
    text, _ := reader.ReadString('\n')
    text = strings.TrimSpace(text)
    return strings.ToLower(text) == "y"
}

func promptInterval() int {
    fmt.Printf("Enter interval in minutes (default %d): ", defaultMinutes)
    reader := bufio.NewReader(os.Stdin)
    text, _ := reader.ReadString('\n')
    text = strings.TrimSpace(text)
    if text == "" {
        return defaultMinutes
    }
    if v, err := strconv.Atoi(text); err == nil && v > 0 {
        return v
    }
    return defaultMinutes
}

func randomTimestamp() string {
    return strconv.FormatInt(time.Now().UnixNano(), 16) + "-" + strconv.Itoa(rand.Intn(1000000))
}

func fetchBaselines(runningDir string) {
    ts := time.Now().Format("Jan 02, 2006 - 03:04PM")
    fmt.Println("Baseline mode: Fetching remote CSVs, PDFs, and images as initial baselines...")

    dir, err := os.Open(runningDir)
    if err != nil {
        fmt.Printf("[%s] Error opening directory: %v\n", ts, err)
        return
    }
    defer dir.Close()

    names, err := dir.Readdirnames(-1)
    if err != nil {
        fmt.Printf("[%s] Error reading directory: %v\n", ts, err)
        return
    }

    var baselineFiles []string
    for _, filename := range names {
        ext := strings.ToLower(filepath.Ext(filename))
        if ext == ".csv" || ext == ".pdf" {
            baselineFiles = append(baselineFiles, filename)
        } else {
            for _, imgExt := range imageExts {
                if ext == imgExt && strings.Contains(strings.ToLower(filename), "map") {
                    baselineFiles = append(baselineFiles, filename)
                    break
                }
            }
        }
    }

    for i, originalFilename := range baselineFiles {
        localPath := filepath.Join(runningDir, originalFilename)
        encodedFilename := url.PathEscape(originalFilename)
        rawURL := baseURL + encodedFilename + "?t=" + randomTimestamp()

        if i > 0 {
            time.Sleep(fetchPause)
        }

        client := &http.Client{
            Timeout: 30 * time.Second,
        }
        req, err := http.NewRequest("GET", rawURL, nil)
        if err != nil {
            fmt.Printf("[%s] %s: Baseline request failed: %v\n", ts, originalFilename, err)
            continue
        }
        req.Header.Set("User-Agent", "Googlebot/2.1; +http://www.google.com/bot.html")
        req.Header.Set("Cache-Control", "no-cache, no-store, must-revalidate")
        req.Header.Set("Pragma", "no-cache")
        req.Header.Set("Expires", "0")
        req.Header.Set("If-Modified-Since", "Thu, 01 Jan 1970 00:00:00 GMT")
        req.Header.Set("If-None-Match", "")
        req.Header.Set("Connection", "close")
        req.Header.Del("Accept-Encoding")

        resp, err := client.Do(req)
        if err != nil {
            fmt.Printf("[%s] %s: Baseline fetch failed: %v\n", ts, originalFilename, err)
            continue
        }
        defer resp.Body.Close()

        if resp.StatusCode != http.StatusOK {
            fmt.Printf("[%s] %s: Baseline HTTP %d (URL: %s)\n", ts, originalFilename, resp.StatusCode, rawURL)
            continue
        }

        body, err := io.ReadAll(resp.Body)
        if err != nil {
            fmt.Printf("[%s] %s: Baseline read failed: %v\n", ts, originalFilename, err)
            continue
        }

        err = os.WriteFile(localPath, body, 0644)
        if err != nil {
            fmt.Printf("[%s] %s: Baseline save failed: %v\n", ts, originalFilename, err)
            continue
        }

        fmt.Printf("[%s] %s: Baseline saved\n", ts, originalFilename)
    }
}

func runCycle(interval int, runningDir string) {
    ts := time.Now().Format("Jan 02, 2006 - 03:04PM")
    fmt.Printf("[%s] Starting cycle, scanning %s for .csv, .pdf, and image files\n", ts, runningDir)

    var csvFiles, pdfFiles, imageFiles []string
    dir, err := os.Open(runningDir)
    if err != nil {
        fmt.Printf("[%s] Error opening directory: %v\n", ts, err)
        return
    }
    defer dir.Close()

    names, err := dir.Readdirnames(-1)
    if err != nil {
        fmt.Printf("[%s] Error reading directory: %v\n", ts, err)
        return
    }

    for _, filename := range names {
        ext := strings.ToLower(filepath.Ext(filename))
        if ext == ".csv" {
            csvFiles = append(csvFiles, filepath.Join(runningDir, filename))
        } else if ext == ".pdf" {
            pdfFiles = append(pdfFiles, filepath.Join(runningDir, filename))
        } else {
            for _, imgExt := range imageExts {
                if ext == imgExt && strings.Contains(strings.ToLower(filename), "map") {
                    imageFiles = append(imageFiles, filepath.Join(runningDir, filename))
                    break
                }
            }
        }
    }

    fmt.Printf("[%s] Found %d .csv files, %d .pdf files, and %d image files\n", ts, len(csvFiles), len(pdfFiles), len(imageFiles))

    var filenames []string
    for _, localPath := range csvFiles {
        filename := filepath.Base(localPath)
        filenames = append(filenames, filename)
    }
    for _, localPath := range pdfFiles {
        filename := filepath.Base(localPath)
        filenames = append(filenames, filename)
    }
    for _, localPath := range imageFiles {
        filename := filepath.Base(localPath)
        filenames = append(filenames, filename)
    }

    sort.Strings(filenames)

    var unifiedBuilder strings.Builder
    var shiftLog []string

    for i, originalFilename := range filenames {
        localPath := filepath.Join(runningDir, originalFilename)
        encodedFilename := url.PathEscape(originalFilename)
        rawURL := baseURL + encodedFilename + "?t=" + randomTimestamp()

        if i > 0 {
            time.Sleep(fetchPause)
        }

        var rawHash string
        var body []byte
        var diffText string

        client := &http.Client{
            Timeout: 30 * time.Second,
        }
        req, err := http.NewRequest("GET", rawURL, nil)
        if err != nil {
            fmt.Printf("[%s] %s: Request failed: %v (URL: %s)\n", ts, originalFilename, err, rawURL)
            rawHash = zeroHash
            continue
        }
        req.Header.Set("User-Agent", "Googlebot/2.1; +http://www.google.com/bot.html")
        req.Header.Set("Cache-Control", "no-cache, no-store, must-revalidate")
        req.Header.Set("Pragma", "no-cache")
        req.Header.Set("Expires", "0")
        req.Header.Set("If-Modified-Since", "Thu, 01 Jan 1970 00:00:00 GMT")
        req.Header.Set("If-None-Match", "")
        req.Header.Set("Connection", "close")
        req.Header.Del("Accept-Encoding")

        resp, err := client.Do(req)
        if err != nil {
            fmt.Printf("[%s] %s: Fetch failed: %v (URL: %s)\n", ts, originalFilename, err, rawURL)
            rawHash = zeroHash
            continue
        }
        defer resp.Body.Close()

        if resp.StatusCode != http.StatusOK {
            fmt.Printf("[%s] %s: SHIFT DETECTED! Remote unavailable (HTTP %d) (URL: %s)\n", ts, originalFilename, resp.StatusCode, rawURL)
            diffText = fmt.Sprintf("[%s] %s: Remote unavailable (HTTP %d) - potential deletion or rename (URL: %s)\n", ts, originalFilename, resp.StatusCode, rawURL)
            shiftLog = append(shiftLog, diffText)
            rawHash = zeroHash
            continue
        }

        body, err = io.ReadAll(resp.Body)
        if err != nil {
            fmt.Printf("[%s] %s: Read failed: %v\n", ts, originalFilename, err)
            rawHash = zeroHash
            continue
        }

        rawHash = sha256Hex(body)
        localHash, localErr := fileHash(localPath)
        if localErr != nil {
            fmt.Printf("[%s] %s: Local hash failed: %v\n", ts, originalFilename, localErr)
            continue
        }

        ext := strings.ToLower(filepath.Ext(originalFilename))
        if rawHash == localHash {
            fmt.Printf("[%s] %s: No change (hash: %s)\n", ts, originalFilename, rawHash[:8])
        } else {
            fmt.Printf("[%s] %s: SHIFT DETECTED! Logging diff to %s\n", ts, originalFilename, logFile)
            if ext == ".csv" {
                diffText = generateCSVDiff(localPath, body, ts, originalFilename)
            } else if ext == ".pdf" {
                diffText = generatePDFDiff(localPath, body, ts, originalFilename)
            } else {
                localExif, localOcr, exifErr, ocrErr := extractImageData(localPath)
                remoteExif, remoteOcr, remoteExifErr, remoteOcrErr := extractImageDataFromBytes(body, originalFilename)

                diffText = generateImageDiff(localPath, body, localExif, remoteExif, localOcr, remoteOcr, exifErr, remoteExifErr, ocrErr, remoteOcrErr, ts, originalFilename)
            }
            if len(diffText) > maxDiffChars {
                diffText = diffText[:maxDiffChars] + "... (truncated)"
            }
            shiftLog = append(shiftLog, diffText)

            changedPath := filepath.Join(runningDir, originalFilename+"_"+strings.ReplaceAll(ts, " ", "_")+".changed")
            err = os.WriteFile(changedPath, body, 0644)
            if err != nil {
                fmt.Printf("[%s] %s: Save changed file failed: %v\n", ts, originalFilename, err)
            }
        }

        unifiedBuilder.WriteString(rawHash)
    }

    if len(shiftLog) > 0 {
        logFileHandle, err := os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
        if err != nil {
            fmt.Printf("[%s] Error opening log file: %v\n", ts, err)
        } else {
            for _, diff := range shiftLog {
                logFileHandle.WriteString(diff + "\n")
            }
            logFileHandle.Close()
        }
    }

    unifiedHash := sha256Hex([]byte(unifiedBuilder.String()))
    fmt.Printf("[%s] Cycle complete for %d files\n", ts, len(filenames))
    fmt.Printf("[%s] Unified Hash: 0x%s\n", ts, unifiedHash)
}

func generateCSVDiff(localPath string, remoteData []byte, ts, filename string) string {
    localCSV, err := parseCSV(localPath)
    if err != nil {
        return fmt.Sprintf("[%s] CSV: %s | Error parsing local: %v", ts, filename, err)
    }
    remoteCSV, err := parseCSVFromBytes(remoteData)
    if err != nil {
        return fmt.Sprintf("[%s] CSV: %s | Error parsing remote: %v", ts, filename, err)
    }

    var diff strings.Builder
    diff.WriteString(fmt.Sprintf("[%s] Diff for %s\n", ts, filename))

    addedCount, omittedCount, modifiedCount := 0, 0, 0
    maxChanges := maxDiffChanges

    maxRows := len(localCSV)
    if len(remoteCSV) > maxRows {
        maxRows = len(remoteCSV)
    }

    for i := 0; i < maxRows && addedCount+omittedCount+modifiedCount < maxChanges; i++ {
        localRow := []string{}
        if i < len(localCSV) {
            localRow = localCSV[i]
        }
        remoteRow := []string{}
        if i < len(remoteCSV) {
            remoteRow = remoteCSV[i]
        }

        if len(localRow) == 0 {
            addedCount++
            diff.WriteString(fmt.Sprintf("Added row %d: %s\n", i+1, strings.Join(remoteRow, ",")))
        } else if len(remoteRow) == 0 {
            omittedCount++
            diff.WriteString(fmt.Sprintf("Omitted row %d: %s\n", i+1, strings.Join(localRow, ",")))
        } else {
            maxCols := len(localRow)
            if len(remoteRow) > maxCols {
                maxCols = len(remoteRow)
            }
            for j := 0; j < maxCols; j++ {
                localField := ""
                if j < len(localRow) {
                    localField = localRow[j]
                }
                remoteField := ""
                if j < len(remoteRow) {
                    remoteField = remoteRow[j]
                }
                if localField != remoteField {
                    modifiedCount++
                    diff.WriteString(fmt.Sprintf("Modified field in row %d, col %d: '%s' → '%s'\n", i+1, j+1, localField, remoteField))
                    break
                }
            }
        }
    }

    if addedCount+omittedCount+modifiedCount == 0 {
        diff.WriteString("No specific changes identified (full content mismatch)\n")
    }

    return diff.String()
}

func generatePDFDiff(localPath string, remoteData []byte, ts, filename string) string {
    localHash, _ := fileHash(localPath)
    remoteHash := sha256Hex(remoteData)
    return fmt.Sprintf("[%s] PDF Diff for %s\nFile Hash: Local=%s, Remote=%s\n", ts, filename, localHash, remoteHash)
}

func generateTextDiff(localText, remoteText, section string) string {
    var diff strings.Builder

    localLines := strings.Split(localText, "\n")
    remoteLines := strings.Split(remoteText, "\n")

    addedCount, omittedCount, modifiedCount := 0, 0, 0
    maxChanges := maxDiffChanges

    maxLines := len(localLines)
    if len(remoteLines) > maxLines {
        maxLines = len(remoteLines)
    }

    for i := 0; i < maxLines && addedCount+omittedCount+modifiedCount < maxChanges; i++ {
        localLine := ""
        if i < len(localLines) {
            localLine = strings.TrimSpace(localLines[i])
        }
        remoteLine := ""
        if i < len(remoteLines) {
            remoteLine = strings.TrimSpace(remoteLines[i])
        }

        if localLine == "" && remoteLine != "" {
            addedCount++
            diff.WriteString(fmt.Sprintf("Added %s line %d: %s\n", section, i+1, remoteLine))
        } else if remoteLine == "" && localLine != "" {
            omittedCount++
            diff.WriteString(fmt.Sprintf("Omitted %s line %d: %s\n", section, i+1, localLine))
        } else if localLine != remoteLine {
            modifiedCount++
            diff.WriteString(fmt.Sprintf("Modified %s line %d: '%s' → '%s'\n", section, i+1, localLine, remoteLine))
        }
    }

    if addedCount+omittedCount+modifiedCount == 0 {
        diff.WriteString(fmt.Sprintf("No %s changes identified\n", section))
    }

    return diff.String()
}

func generateImageDiff(localPath string, remoteData []byte, localExif, remoteExif, localOcr, remoteOcr string, localExifErr, remoteExifErr, localOcrErr, remoteOcrErr error, ts, filename string) string {
    var diff strings.Builder
    diff.WriteString(fmt.Sprintf("[%s] Image Diff for %s\n", ts, filename))

    localHash, _ := fileHash(localPath)
    remoteHash := sha256Hex(remoteData)
    diff.WriteString(fmt.Sprintf("File Hash: Local=%s, Remote=%s\n", localHash, remoteHash))

    if localExifErr != nil {
        diff.WriteString(fmt.Sprintf("Local EXIF Error: %v\n", localExifErr))
    }
    if remoteExifErr != nil {
        diff.WriteString(fmt.Sprintf("Remote EXIF Error: %v\n", remoteExifErr))
    }
    if localExifErr == nil && remoteExifErr == nil {
        if localExif != remoteExif {
            diff.WriteString("EXIF Changed:\n")
            diff.WriteString(generateTextDiff(localExif, remoteExif, "EXIF"))
        } else {
            diff.WriteString("EXIF Unchanged\n")
        }
    } else if localExifErr == nil && remoteExifErr != nil {
        diff.WriteString(fmt.Sprintf("EXIF: Local present, remote extraction failed\nLocal EXIF:\n%s\n", localExif))
    } else if localExifErr != nil && remoteExifErr == nil {
        diff.WriteString(fmt.Sprintf("EXIF: Remote added, local extraction failed\nRemote EXIF:\n%s\n", remoteExif))
    }

    if localOcrErr != nil {
        diff.WriteString(fmt.Sprintf("Local OCR Error: %v\n", localOcrErr))
    }
    if remoteOcrErr != nil {
        diff.WriteString(fmt.Sprintf("Remote OCR Error: %v\n", remoteOcrErr))
    }
    if localOcrErr == nil && remoteOcrErr == nil {
        if localOcr != remoteOcr {
            diff.WriteString("OCR Text Changed:\n")
            diff.WriteString(generateTextDiff(localOcr, remoteOcr, "OCR"))
        } else {
            diff.WriteString("OCR Text Unchanged\n")
        }
    } else if localOcrErr == nil && remoteOcrErr != nil {
        diff.WriteString(fmt.Sprintf("OCR: Local present, remote extraction failed\nLocal OCR Text:\n%s\n", localOcr))
    } else if localOcrErr != nil && remoteOcrErr == nil {
        diff.WriteString(fmt.Sprintf("OCR: Remote added, local extraction failed\nRemote OCR Text:\n%s\n", remoteOcr))
    }

    return diff.String()
}

func parseCSV(path string) ([][]string, error) {
    f, err := os.Open(path)
    if err != nil {
        return nil, err
    }
    defer f.Close()

    reader := csv.NewReader(f)
    return reader.ReadAll()
}

func parseCSVFromBytes(data []byte) ([][]string, error) {
    reader := csv.NewReader(strings.NewReader(string(data)))
    return reader.ReadAll()
}

func fileHash(localPath string) (string, error) {
    f, err := os.Open(localPath)
    if err != nil {
        return "", err
    }
    defer f.Close()

    h := sha256.New()
    _, err = io.Copy(h, f)
    if err != nil {
        return "", err
    }
    return hex.EncodeToString(h.Sum(nil)), nil
}

func sha256Hex(b []byte) string {
    h := sha256.New()
    h.Write(b)
    return hex.EncodeToString(h.Sum(nil))
}

func extractImageData(localPath string) (exifData, ocrText string, exifErr, ocrErr error) {
    ext := strings.ToLower(filepath.Ext(localPath))

    f, err := os.Open(localPath)
    if err != nil {
        exifErr = err
        return
    }
    defer f.Close()
    exifData, exifErr = extractExif(f)

    if ext == ".avif" {
        ocrErr = fmt.Errorf("AVIF format not supported for OCR")
        return
    }

    client := gosseract.NewClient()
    defer client.Close()
    client.SetImage(localPath)
    ocrText, ocrErr = client.Text()

    return exifData, ocrText, exifErr, ocrErr
}

func extractImageDataFromBytes(data []byte, filename string) (exifData, ocrText string, exifErr, ocrErr error) {
    ext := strings.ToLower(filepath.Ext(filename))

    tempFile := filepath.Join(os.TempDir(), "remote_"+filename)
    err := os.WriteFile(tempFile, data, 0644)
    if err != nil {
        exifErr = err
        ocrErr = err
        return
    }
    defer os.Remove(tempFile)

    f, err := os.Open(tempFile)
    if err != nil {
        exifErr = err
        return
    }
    defer f.Close()
    exifData, exifErr = extractExif(f)

    if ext == ".avif" {
        ocrErr = fmt.Errorf("AVIF format not supported for OCR")
        return
    }

    client := gosseract.NewClient()
    defer client.Close()
    client.SetImage(tempFile)
    ocrText, ocrErr = client.Text()

    return exifData, ocrText, exifErr, ocrErr
}

type exifWalker struct {
    builder strings.Builder
}

func (w *exifWalker) Walk(name exifpkg.FieldName, tag *tiff.Tag) error {
    val, err := tag.StringVal()
    if err != nil {
        return nil
    }
    w.builder.WriteString(fmt.Sprintf("%s: %s\n", name, val))
    return nil
}

func extractExif(f *os.File) (string, error) {
    _, err := f.Seek(0, 0)
    if err != nil {
        return "", err
    }
    x, err := exifpkg.Decode(f)
    if err != nil {
        return "", err
    }

    walker := &exifWalker{}
    err = x.Walk(walker)
    if err != nil {
        return "", err
    }
    return walker.builder.String(), nil
}

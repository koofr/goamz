package s3

import (
	"encoding/xml"
	"errors"
	"io"
	"sort"
	"strconv"
	"time"
)

// Multi represents an unfinished multipart upload.
//
// Multipart uploads allow sending big objects in smaller chunks.
// After all parts have been sent, the upload must be explicitly
// completed by calling Complete with the list of parts.
//
// See http://goo.gl/vJfTG for an overview of multipart uploads.
type Multi struct {
	Bucket    *Bucket
	Key       string
	UploadId  string
	Initiated *time.Time
}

// That's the default. Here just for testing.
var listMultiMax = 1000

type listMultiResp struct {
	NextKeyMarker      string
	NextUploadIdMarker string
	IsTruncated        bool
	Upload             []Multi
	CommonPrefixes     []string `xml:"CommonPrefixes>Prefix"`
}

// ListMulti returns the list of unfinished multipart uploads in b.
//
// The prefix parameter limits the response to keys that begin with the
// specified prefix. You can use prefixes to separate a bucket into different
// groupings of keys (to get the feeling of folders, for example).
//
// The delim parameter causes the response to group all of the keys that
// share a common prefix up to the next delimiter in a single entry within
// the CommonPrefixes field. You can use delimiters to separate a bucket
// into different groupings of keys, similar to how folders would work.
//
// See http://goo.gl/ePioY for details.
func (b *Bucket) ListMulti(prefix, delim string) (multis []*Multi, prefixes []string, err error) {
	params := map[string][]string{
		"uploads":     {},
		"max-uploads": {strconv.FormatInt(int64(listMultiMax), 10)},
		"prefix":      {prefix},
		"delimiter":   {delim},
	}
	for attempt := attempts.Start(); attempt.Next(); {
		req := &request{
			method: "GET",
			bucket: b.Name,
			params: params,
		}
		var resp listMultiResp
		err := b.S3.query(req, &resp)
		if shouldRetry(err) && attempt.HasNext() {
			continue
		}
		if err != nil {
			return nil, nil, err
		}
		for i := range resp.Upload {
			multi := &resp.Upload[i]
			multi.Bucket = b
			multis = append(multis, multi)
		}
		prefixes = append(prefixes, resp.CommonPrefixes...)
		if !resp.IsTruncated {
			return multis, prefixes, nil
		}
		params["key-marker"] = []string{resp.NextKeyMarker}
		params["upload-id-marker"] = []string{resp.NextUploadIdMarker}
		attempt = attempts.Start() // Last request worked.
	}
	panic("unreachable")
}

// Multi returns a multipart upload handler for the provided key
// inside b. If a multipart upload exists for key, it is returned,
// otherwise a new multipart upload is initiated with contType and perm.
func (b *Bucket) Multi(key, contType string, perm ACL) (*Multi, error) {
	multis, _, err := b.ListMulti(key, "")
	if err != nil && !hasCode(err, "NoSuchUpload") {
		return nil, err
	}
	for _, m := range multis {
		if m.Key == key {
			return m, nil
		}
	}
	return b.InitMulti(key, contType, perm)
}

// InitMulti initializes a new multipart upload at the provided
// key inside b and returns a value for manipulating it.
//
// See http://goo.gl/XP8kL for details.
func (b *Bucket) InitMulti(key string, contType string, perm ACL) (*Multi, error) {
	headers := map[string][]string{
		"Content-Type":   {contType},
		"Content-Length": {"0"},
		"x-amz-acl":      {string(perm)},
	}
	params := map[string][]string{
		"uploads": {},
	}
	req := &request{
		method:  "POST",
		bucket:  b.Name,
		path:    key,
		headers: headers,
		params:  params,
	}
	var err error
	var resp struct {
		UploadId string `xml:"UploadId"`
	}
	for attempt := attempts.Start(); attempt.Next(); {
		err = b.S3.query(req, &resp)
		if !shouldRetry(err) {
			break
		}
	}
	if err != nil {
		return nil, err
	}
	return &Multi{Bucket: b, Key: key, UploadId: resp.UploadId}, nil
}

// PutPartHash sends part n of the multipart upload, reading all the content from r
// with partSize and MD5 base64 encoded hash.
// Each part, except for the last one, must be at least 5MB in size.
//
// See http://goo.gl/pqZer for details.
func (m *Multi) PutPartHash(n int, r io.ReadSeeker, partSize int64, md5b64 string, sha256hex string) (Part, error) {
	headers := map[string][]string{
		"Content-Length": {strconv.FormatInt(partSize, 10)},
		"Content-MD5":    {md5b64},
	}
	params := map[string][]string{
		"uploadId":   {m.UploadId},
		"partNumber": {strconv.FormatInt(int64(n), 10)},
	}
	for attempt := attempts.Start(); attempt.Next(); {
		_, err := r.Seek(0, 0)
		if err != nil {
			return Part{}, err
		}
		req := &request{
			method:  "PUT",
			bucket:  m.Bucket.Name,
			path:    m.Key,
			headers: headers,
			params:  params,
			payload: payload{
				payload:   r,
				md5b64:    md5b64,
				sha256hex: sha256hex,
			},
		}
		err = m.Bucket.S3.prepare(req)
		if err != nil {
			return Part{}, err
		}
		hresp, err := m.Bucket.S3.run(req)
		if shouldRetry(err) && attempt.HasNext() {
			continue
		}
		if err != nil {
			return Part{}, err
		}
		hresp.Body.Close()
		etag := hresp.Header.Get("ETag")
		if etag == "" {
			return Part{}, errors.New("part upload succeeded with no ETag")
		}
		return Part{n, etag, partSize}, nil
	}
	panic("unreachable")
}

type Part struct {
	N    int `xml:"PartNumber"`
	ETag string
	Size int64
}

type partSlice []Part

func (s partSlice) Len() int           { return len(s) }
func (s partSlice) Less(i, j int) bool { return s[i].N < s[j].N }
func (s partSlice) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }

type listPartsResp struct {
	NextPartNumberMarker string
	IsTruncated          bool
	Part                 []Part
}

// That's the default. Here just for testing.
var listPartsMax = 1000

// ListParts returns the list of previously uploaded parts in m,
// ordered by part number.
//
// See http://goo.gl/ePioY for details.
func (m *Multi) ListParts() ([]Part, error) {
	params := map[string][]string{
		"uploadId":  {m.UploadId},
		"max-parts": {strconv.FormatInt(int64(listPartsMax), 10)},
	}
	var parts partSlice
	for attempt := attempts.Start(); attempt.Next(); {
		req := &request{
			method: "GET",
			bucket: m.Bucket.Name,
			path:   m.Key,
			params: params,
		}
		var resp listPartsResp
		err := m.Bucket.S3.query(req, &resp)
		if shouldRetry(err) && attempt.HasNext() {
			continue
		}
		if err != nil {
			return nil, err
		}
		parts = append(parts, resp.Part...)
		if !resp.IsTruncated {
			sort.Sort(parts)
			return parts, nil
		}
		params["part-number-marker"] = []string{resp.NextPartNumberMarker}
		attempt = attempts.Start() // Last request worked.
	}
	panic("unreachable")
}

type ReaderAtSeeker interface {
	io.ReaderAt
	io.ReadSeeker
}

type completeUpload struct {
	XMLName xml.Name      `xml:"CompleteMultipartUpload"`
	Parts   completeParts `xml:"Part"`
}

type completePart struct {
	PartNumber int
	ETag       string
}

type completeParts []completePart

func (p completeParts) Len() int           { return len(p) }
func (p completeParts) Less(i, j int) bool { return p[i].PartNumber < p[j].PartNumber }
func (p completeParts) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// Complete assembles the given previously uploaded parts into the
// final object. This operation may take several minutes.
//
// See http://goo.gl/2Z7Tw for details.
func (m *Multi) Complete(parts []Part) error {
	params := map[string][]string{
		"uploadId": {m.UploadId},
	}
	c := completeUpload{}
	for _, p := range parts {
		c.Parts = append(c.Parts, completePart{p.N, p.ETag})
	}
	sort.Sort(c.Parts)
	data, err := xml.Marshal(&c)
	if err != nil {
		return err
	}
	headers := map[string][]string{
		"Content-Length": {strconv.FormatInt(int64(len(data)), 10)},
	}
	for attempt := attempts.Start(); attempt.Next(); {
		req := &request{
			method:  "POST",
			bucket:  m.Bucket.Name,
			path:    m.Key,
			headers: headers,
			params:  params,
			payload: getPayload(data),
		}
		err := m.Bucket.S3.query(req, nil)
		if shouldRetry(err) && attempt.HasNext() {
			continue
		}
		return err
	}
	panic("unreachable")
}

// Abort deletes an unifinished multipart upload and any previously
// uploaded parts for it.
//
// After a multipart upload is aborted, no additional parts can be
// uploaded using it. However, if any part uploads are currently in
// progress, those part uploads might or might not succeed. As a result,
// it might be necessary to abort a given multipart upload multiple
// times in order to completely free all storage consumed by all parts.
//
// NOTE: If the described scenario happens to you, please report back to
// the goamz authors with details. In the future such retrying should be
// handled internally, but it's not clear what happens precisely (Is an
// error returned? Is the issue completely undetectable?).
//
// See http://goo.gl/dnyJw for details.
func (m *Multi) Abort() error {
	params := map[string][]string{
		"uploadId": {m.UploadId},
	}
	for attempt := attempts.Start(); attempt.Next(); {
		req := &request{
			method: "DELETE",
			bucket: m.Bucket.Name,
			path:   m.Key,
			params: params,
		}
		err := m.Bucket.S3.query(req, nil)
		if shouldRetry(err) && attempt.HasNext() {
			continue
		}
		return err
	}
	panic("unreachable")
}

package s3

import (
	"bytes"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/koofr/goamz/aws"
)

// Common date formats for signing requests
const (
	ISO8601BasicFormat      = "20060102T150405Z"
	ISO8601BasicFormatShort = "20060102"
)

/*
The V4Signer encapsulates all of the functionality to sign a request with the AWS
Signature Version 4 Signing Process. (http://goo.gl/u1OWZz)
*/
type V4Signer struct {
	auth        aws.Auth
	serviceName string
	region      aws.Region
}

/*
Return a new instance of a V4Signer capable of signing AWS requests.
*/
func NewV4Signer(auth aws.Auth, serviceName string, region aws.Region) *V4Signer {
	return &V4Signer{
		auth:        auth,
		serviceName: serviceName,
		region:      region,
	}
}

/*
Sign a request according to the AWS Signature Version 4 Signing Process. (http://goo.gl/u1OWZz)
The signed request will include an "x-amz-date" header with a current timestamp if a valid "x-amz-date"
or "date" header was not available in the original request. In addition, AWS Signature Version 4 requires
the "host" header to be a signed header, therefor the Sign method will manually set a "host" header from
the request.Host.
The signed request will include a new "Authorization" header indicating that the request has been signed.
Any changes to the request after signing the request will invalidate the signature.
*/
func (s *V4Signer) Sign(req *http.Request, payloadHash string) (err error) {
	if payloadHash == "" {
		payloadHash = EmptyStringSHA256Hex
	}

	req.Header.Set("host", req.Host) // host header must be included as a signed header
	t := s.requestTime(req)          // Get request time

	if _, ok := req.Form["X-Amz-Expires"]; ok {
		// We are authenticating the the request by using query params
		// (also known as pre-signing a url, http://docs.aws.amazon.com/AmazonS3/latest/API/sigv4-query-string-auth.html)
		payloadHash = "UNSIGNED-PAYLOAD"
		req.Header.Del("x-amz-date")

		req.Form["X-Amz-SignedHeaders"] = []string{s.signedHeaders(req.Header)}
		req.Form["X-Amz-Algorithm"] = []string{"AWS4-HMAC-SHA256"}
		req.Form["X-Amz-Credential"] = []string{s.auth.AccessKey + "/" + s.credentialScope(t)}
		req.Form["X-Amz-Date"] = []string{t.Format(ISO8601BasicFormat)}
		req.URL.RawQuery = req.Form.Encode()
	} else {
		req.Header.Set("x-amz-content-sha256", payloadHash) // x-amz-content-sha256 contains the payload hash
	}
	creq, err := s.canonicalRequest(req, payloadHash) // Build canonical request
	if err != nil {
		return err
	}
	sts := s.stringToSign(t, creq)                    // Build string to sign
	signature := s.signature(t, sts)                  // Calculate the AWS Signature Version 4
	auth := s.authorization(req.Header, t, signature) // Create Authorization header value

	if _, ok := req.Form["X-Amz-Expires"]; ok {
		req.Form["X-Amz-Signature"] = []string{signature}
	} else {
		req.Header.Set("Authorization", auth) // Add Authorization header to request
	}
	return nil
}

/*
requestTime method will parse the time from the request "x-amz-date" or "date" headers.
If the "x-amz-date" header is present, that will take priority over the "date" header.
If neither header is defined or we are unable to parse either header as a valid date
then we will create a new "x-amz-date" header with the current time.
*/
func (s *V4Signer) requestTime(req *http.Request) time.Time {
	// Get "x-amz-date" header
	date := req.Header.Get("x-amz-date")

	// Attempt to parse as ISO8601BasicFormat
	t, err := time.Parse(ISO8601BasicFormat, date)
	if err == nil {
		return t
	}

	// Attempt to parse as http.TimeFormat
	t, err = time.Parse(http.TimeFormat, date)
	if err == nil {
		req.Header.Set("x-amz-date", t.Format(ISO8601BasicFormat))
		return t
	}

	// Get "date" header
	date = req.Header.Get("date")

	// Attempt to parse as http.TimeFormat
	t, err = time.Parse(http.TimeFormat, date)
	if err == nil {
		return t
	}

	// Create a current time header to be used
	t = time.Now().UTC()
	req.Header.Set("x-amz-date", t.Format(ISO8601BasicFormat))
	return t
}

/*
canonicalRequest method creates the canonical request according to Task 1 of the AWS Signature Version 4 Signing Process. (http://goo.gl/eUUZ3S)
    CanonicalRequest =
      HTTPRequestMethod + '\n' +
      CanonicalURI + '\n' +
      CanonicalQueryString + '\n' +
      CanonicalHeaders + '\n' +
      SignedHeaders + '\n' +
      HexEncode(Hash(Payload))
payloadHash is optional; use the empty string and it will be calculated from the request
*/
func (s *V4Signer) canonicalRequest(req *http.Request, payloadHash string) (string, error) {
	c := new(bytes.Buffer)
	fmt.Fprintf(c, "%s\n", req.Method)
	fmt.Fprintf(c, "%s\n", s.canonicalURI(req.URL))
	fmt.Fprintf(c, "%s\n", s.canonicalQueryString(req.URL))
	fmt.Fprintf(c, "%s\n\n", s.canonicalHeaders(req.Header))
	fmt.Fprintf(c, "%s\n", s.signedHeaders(req.Header))
	fmt.Fprintf(c, "%s", payloadHash)
	return c.String(), nil
}

func (s *V4Signer) canonicalURI(u *url.URL) string {
	u = &url.URL{Path: u.Path}
	canonicalPath := u.String()

	slash := strings.HasSuffix(canonicalPath, "/")
	canonicalPath = path.Clean(canonicalPath)

	if canonicalPath == "" || canonicalPath == "." {
		canonicalPath = "/"
	}

	if canonicalPath != "/" && slash {
		canonicalPath += "/"
	}

	return canonicalPath
}

func (s *V4Signer) canonicalQueryString(u *url.URL) string {
	keyValues := make(map[string]string, len(u.Query()))
	keys := make([]string, len(u.Query()))

	key_i := 0
	for k, vs := range u.Query() {
		k = url.QueryEscape(k)

		a := make([]string, len(vs))
		for idx, v := range vs {
			v = url.QueryEscape(v)
			a[idx] = fmt.Sprintf("%s=%s", k, v)
		}

		keyValues[k] = strings.Join(a, "&")
		keys[key_i] = k
		key_i++
	}

	sort.Strings(keys)

	query := make([]string, len(keys))
	for idx, key := range keys {
		query[idx] = keyValues[key]
	}

	query_str := strings.Join(query, "&")

	// AWS V4 signing requires that the space characters
	// are encoded as %20 instead of +. On the other hand
	// golangs url.QueryEscape as well as url.Values.Encode()
	// both encode the space as a + character. See:
	// http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
	// https://github.com/golang/go/issues/4013
	// https://groups.google.com/forum/#!topic/golang-nuts/BB443qEjPIk

	return strings.Replace(query_str, "+", "%20", -1)
}

func (s *V4Signer) canonicalHeaders(h http.Header) string {
	i, a, lowerCase := 0, make([]string, len(h)), make(map[string][]string)

	for k, v := range h {
		lowerCase[strings.ToLower(k)] = v
	}

	var keys []string
	for k := range lowerCase {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		v := lowerCase[k]
		for j, w := range v {
			v[j] = strings.Trim(w, " ")
		}
		sort.Strings(v)
		a[i] = strings.ToLower(k) + ":" + strings.Join(v, ",")
		i++
	}
	return strings.Join(a, "\n")
}

func (s *V4Signer) signedHeaders(h http.Header) string {
	i, a := 0, make([]string, len(h))
	for k := range h {
		a[i] = strings.ToLower(k)
		i++
	}
	sort.Strings(a)
	return strings.Join(a, ";")
}

/*
stringToSign method creates the string to sign accorting to Task 2 of the AWS Signature Version 4 Signing Process. (http://goo.gl/es1PAu)
    StringToSign  =
      Algorithm + '\n' +
      RequestDate + '\n' +
      CredentialScope + '\n' +
      HexEncode(Hash(CanonicalRequest))
*/
func (s *V4Signer) stringToSign(t time.Time, creq string) string {
	w := new(bytes.Buffer)
	fmt.Fprint(w, "AWS4-HMAC-SHA256\n")
	fmt.Fprintf(w, "%s\n", t.Format(ISO8601BasicFormat))
	fmt.Fprintf(w, "%s\n", s.credentialScope(t))
	fmt.Fprintf(w, "%s", SHA256Hex([]byte(creq)))
	return w.String()
}

func (s *V4Signer) credentialScope(t time.Time) string {
	return fmt.Sprintf("%s/%s/%s/aws4_request", t.Format(ISO8601BasicFormatShort), s.region.Name, s.serviceName)
}

/*
signature method calculates the AWS Signature Version 4 according to Task 3 of the AWS Signature Version 4 Signing Process. (http://goo.gl/j0Yqe1)
	signature = HexEncode(HMAC(derived-signing-key, string-to-sign))
*/
func (s *V4Signer) signature(t time.Time, sts string) string {
	h := HMAC(s.derivedKey(t), []byte(sts))
	return fmt.Sprintf("%x", h)
}

/*
derivedKey method derives a signing key to be used for signing a request.
	kSecret = Your AWS Secret Access Key
    kDate = HMAC("AWS4" + kSecret, Date)
    kRegion = HMAC(kDate, Region)
    kService = HMAC(kRegion, Service)
    kSigning = HMAC(kService, "aws4_request")
*/
func (s *V4Signer) derivedKey(t time.Time) []byte {
	h := HMAC([]byte("AWS4"+s.auth.SecretKey), []byte(t.Format(ISO8601BasicFormatShort)))
	h = HMAC(h, []byte(s.region.Name))
	h = HMAC(h, []byte(s.serviceName))
	h = HMAC(h, []byte("aws4_request"))
	return h
}

/*
authorization method generates the authorization header value.
*/
func (s *V4Signer) authorization(header http.Header, t time.Time, signature string) string {
	w := new(bytes.Buffer)
	fmt.Fprint(w, "AWS4-HMAC-SHA256 ")
	fmt.Fprintf(w, "Credential=%s/%s, ", s.auth.AccessKey, s.credentialScope(t))
	fmt.Fprintf(w, "SignedHeaders=%s, ", s.signedHeaders(header))
	fmt.Fprintf(w, "Signature=%s", signature)
	return w.String()
}

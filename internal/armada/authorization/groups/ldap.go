package groups

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-ldap/ldap/v3"
	log "github.com/sirupsen/logrus"

	"github.com/G-Research/armada/internal/armada/configuration"
)

type GroupLookup interface {
	GetGroupNames(SIDs []string) ([]string, error)
}

type LDAPGroupLookup struct {
	config      configuration.LDAPConfig
	cache       map[string]cacheRecord
	cacheExpiry time.Duration
}

func NewLDAPGroupLookup(config configuration.LDAPConfig) *LDAPGroupLookup {
	return &LDAPGroupLookup{
		config:      config,
		cache:       map[string]cacheRecord{},
		cacheExpiry: 5 * time.Minute,
	}
}

type cacheRecord struct {
	name    string
	created time.Time
}

func (lookup *LDAPGroupLookup) GetGroupNames(SIDs []string) ([]string, error) {
	err := lookup.updateCache(SIDs)
	if err != nil {
		return nil, err
	}

	result := []string{}
	for _, sid := range SIDs {
		record, ok := lookup.cache[sid]
		if ok {
			result = append(result, record.name)
		} else {
			return nil, fmt.Errorf("could not find group name for %s", sid)
		}
	}
	return result, nil
}

func (lookup *LDAPGroupLookup) updateCache(SIDs []string) error {
	deadline := time.Now().Add(-lookup.cacheExpiry)
	missing := []string{}
	for _, sid := range SIDs {
		record, ok := lookup.cache[sid]
		if !ok || record.created.Before(deadline) {
			missing = append(missing, sid)
		}
	}

	groupNames, err := lookup.getGroupNames(missing)
	if err != nil {
		return err
	}

	for sid, name := range groupNames {
		lookup.cache[sid] = cacheRecord{
			name:    name,
			created: time.Now(),
		}
	}
	return nil
}

func (lookup *LDAPGroupLookup) getGroupNames(SIDs []string) (map[string]string, error) {
	l, err := ldap.DialURL(lookup.config.URL)
	if err != nil {
		log.Errorf("LDAP dial error: %s", err)
		return nil, err
	}
	defer l.Close()
	err = l.Bind(lookup.config.Username, lookup.config.Password)
	if err != nil {
		log.Errorf("LDAP bind error %s", err)
		return nil, err
	}
	searchRequest := lookup.createGroupSearch(SIDs)
	sr, err := l.Search(searchRequest)
	if err != nil {
		log.Errorf("LDAP search error %s", err)
		return nil, err
	}
	result := map[string]string{}
	for _, entry := range sr.Entries {
		bytes := entry.GetEqualFoldRawAttributeValue("objectSid")
		sid := decodeSID(bytes)
		result[sid] = entry.GetAttributeValue("cn")
	}
	return result, nil
}

func (lookup *LDAPGroupLookup) createGroupSearch(SIDs []string) *ldap.SearchRequest {
	return ldap.NewSearchRequest(
		lookup.config.GroupSearchBase,
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
		"(&(objectClass=group)(|(objectSID="+
			strings.Join(SIDs, ")(objectSID=")+
			")))",
		[]string{"cn", "objectSid"},
		nil,
	)
}

// Modified from https://github.com/bwmarrin/go-objectsid, newer go compiler requires uints for bit shifts
func decodeSID(b []byte) string {
	revisionLevel := int(b[0])
	subAuthorityCount := int(b[1]) & 0xFF
	var authority uint = 0
	for i := 2; i <= 7; i++ {
		authority = authority | uint(b[i])<<uint(8*(5-(i-2)))
	}

	var offset = 8
	var size = 4
	var subAuthorities []uint
	for i := 0; i < subAuthorityCount; i++ {
		var subAuthority uint
		for k := 0; k < size; k++ {
			subAuthority = subAuthority | (uint(b[offset+k])&0xFF)<<uint(8*k)
		}
		subAuthorities = append(subAuthorities, subAuthority)
		offset += size
	}

	s := fmt.Sprintf("S-%d-%d", revisionLevel, authority)
	for _, v := range subAuthorities {
		s += fmt.Sprintf("-%d", v)
	}
	return s
}

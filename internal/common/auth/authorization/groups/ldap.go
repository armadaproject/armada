package groups

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-ldap/ldap/v3"
	log "github.com/sirupsen/logrus"

	"github.com/armadaproject/armada/internal/common/auth/configuration"
)

type GroupLookup interface {
	GetGroupNames(SIDs []string) ([]string, error)
}

type LDAPGroupLookup struct {
	config      configuration.LDAPConfig
	cache       map[string]cacheRecord
	cacheExpiry time.Duration
	mutex       sync.Mutex
}

func NewLDAPGroupLookup(config configuration.LDAPConfig) *LDAPGroupLookup {
	expiry := config.CacheExpiry
	if expiry == 0 {
		expiry = 5 * time.Minute
	}

	return &LDAPGroupLookup{
		config:      config,
		cache:       map[string]cacheRecord{},
		cacheExpiry: expiry,
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
	result := lookup.readFromCache(SIDs)
	return result, nil
}

func (lookup *LDAPGroupLookup) readFromCache(SIDs []string) []string {
	lookup.mutex.Lock()
	defer lookup.mutex.Unlock()

	result := []string{}
	for _, sid := range SIDs {
		record, ok := lookup.cache[sid]
		if ok && record.name != "" {
			result = append(result, record.name)
		}
	}
	return result
}

func (lookup *LDAPGroupLookup) updateCache(SIDs []string) error {
	missing := lookup.getMissingGroups(SIDs)

	if len(missing) == 0 {
		return nil
	}

	groupNames, err := lookup.getGroupNames(missing)
	if err != nil {
		return err
	}

	lookup.saveInCache(SIDs, groupNames)
	return nil
}

func (lookup *LDAPGroupLookup) saveInCache(SIDs []string, groupNames map[string]string) {
	lookup.mutex.Lock()
	defer lookup.mutex.Unlock()

	for sid, name := range groupNames {
		lookup.cache[sid] = cacheRecord{
			name:    name,
			created: time.Now(),
		}
	}

	for _, sid := range SIDs {
		_, exists := lookup.cache[sid]
		if !exists {
			lookup.cache[sid] = cacheRecord{
				name:    "",
				created: time.Now(),
			}
			log.Warnf("Could not find group name for %s", sid)
		}
	}
}

func (lookup *LDAPGroupLookup) getMissingGroups(SIDs []string) []string {
	lookup.mutex.Lock()
	defer lookup.mutex.Unlock()

	deadline := time.Now().Add(-lookup.cacheExpiry)
	missing := []string{}
	for _, sid := range SIDs {
		record, ok := lookup.cache[sid]
		if !ok || record.created.Before(deadline) {
			missing = append(missing, sid)
		}
	}
	return missing
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

	offset := 8
	size := 4
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

package workload

import (
	"math/big"
	"regexp"
	"strconv"
)

var quantityPattern = regexp.MustCompile(`^([+-]?(?:[0-9]+(?:\.[0-9]*)?|\.[0-9]+))([eE][+-]?[0-9]+|[numkKMGTPE]i?)?$`)

// Kubernetes canonicalizes quantities (1000m -> 1, 1024Ki -> 1Mi). Compare
// rational values only for resource fields, never for commands or environment.
func equalQuantity(a, b string) bool {
	x, ok := quantityValue(a)
	if !ok {
		return false
	}
	y, ok := quantityValue(b)
	return ok && x.Cmp(y) == 0
}
func quantityValue(value string) (*big.Rat, bool) {
	m := quantityPattern.FindStringSubmatch(value)
	if len(m) != 3 {
		return nil, false
	}
	number, ok := new(big.Rat).SetString(m[1])
	if !ok {
		return nil, false
	}
	base, power := int64(10), 0
	switch m[2] {
	case "":
	case "n":
		power = -9
	case "u":
		power = -6
	case "m":
		power = -3
	case "k", "K":
		power = 3
	case "M":
		power = 6
	case "G":
		power = 9
	case "T":
		power = 12
	case "P":
		power = 15
	case "E":
		power = 18
	case "Ki":
		base = 2
		power = 10
	case "Mi":
		base = 2
		power = 20
	case "Gi":
		base = 2
		power = 30
	case "Ti":
		base = 2
		power = 40
	case "Pi":
		base = 2
		power = 50
	case "Ei":
		base = 2
		power = 60
	default:
		var err error
		power, err = strconv.Atoi(m[2][1:])
		if err != nil || power < -18 || power > 18 {
			return nil, false
		}
	}
	exponent := power
	if exponent < 0 {
		exponent = -exponent
	}
	factor := new(big.Rat).SetInt(new(big.Int).Exp(big.NewInt(base), big.NewInt(int64(exponent)), nil))
	if power < 0 {
		number.Quo(number, factor)
	} else {
		number.Mul(number, factor)
	}
	return number, true
}
func resourceFieldsMatch(expected, actual any) bool {
	e, ok := expected.(map[string]any)
	if !ok {
		return false
	}
	a, ok := actual.(map[string]any)
	if !ok {
		return false
	}
	for group, resources := range e {
		values, ok := resources.(map[string]any)
		if !ok {
			return false
		}
		got, ok := a[group].(map[string]any)
		if !ok {
			return false
		}
		for resource, value := range values {
			x, ok := value.(string)
			if !ok {
				return false
			}
			y, ok := got[resource].(string)
			if !ok || !equalQuantity(x, y) {
				return false
			}
		}
	}
	return true
}

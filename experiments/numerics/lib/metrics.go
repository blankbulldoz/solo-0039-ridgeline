package lib

import "math"

// RelErr returns |got-exact|/|exact|. When the exact result is zero it
// returns the absolute error, because relative error against zero is
// undefined: this is precisely the cancellation regime where no relative
// error promise can be made.
func RelErr(got, exact float64) (rel, abs float64, exactZero bool) {
	abs = math.Abs(got - exact)
	if exact == 0 {
		return math.Inf(1), abs, true
	}
	return abs / math.Abs(exact), abs, false
}

// ULP returns the distance between got and exact measured in float64 ULPs at
// the magnitude of exact. Returns +Inf when signs differ or exact is zero.
func ULP(got, exact float64) float64 {
	if exact == 0 || math.Signbit(got) != math.Signbit(exact) {
		if got == exact {
			return 0
		}
		return math.Inf(1)
	}
	// ulp(exact): one unit in the last place at exact's exponent.
	exp := math.Floor(math.Log2(math.Abs(exact)))
	ulp := math.Ldexp(1.0, int(exp)-52)
	return math.Abs(got-exact) / ulp
}

// BoundSequential is the standard a priori bound (n-1)*eps/(1-(n-1)*eps) on
// the relative error of sequential summation, assuming the true sum is far
// from zero. eps = 2^-53 for binary64 round-to-nearest-even.
func BoundSequential(n int) float64 {
	const eps = 1.0 / (1 << 53)
	u := float64(n-1) * eps
	return u / (1 - u)
}

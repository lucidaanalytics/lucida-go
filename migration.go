package lucida

import (
	"bytes"
	"context"
	"encoding/json"
	"slices"
	"time"
)

// MigrationStage is a stage of a migration flag (LC-FLG-24): a multivariate flag whose variants are
// the six stages of moving a data path from an old system to a new one.
type MigrationStage string

// Migration stages, in order.
const (
	MigrationOff       MigrationStage = "off"
	MigrationDualwrite MigrationStage = "dualwrite"
	MigrationShadow    MigrationStage = "shadow"
	MigrationLive      MigrationStage = "live"
	MigrationRampdown  MigrationStage = "rampdown"
	MigrationComplete  MigrationStage = "complete"
)

// MigrationStages lists every stage in order.
var MigrationStages = []MigrationStage{MigrationOff, MigrationDualwrite, MigrationShadow, MigrationLive, MigrationRampdown, MigrationComplete}

// MigrationStageOf is the stage a flag value names; anything else (flag off, unknown) is MigrationOff.
func MigrationStageOf(v any) MigrationStage {
	if s, ok := v.(string); ok && slices.Contains(MigrationStages, MigrationStage(s)) {
		return MigrationStage(s)
	}
	return MigrationOff
}

// MigrationReport describes one migrated operation; Errors and LatencyMS are keyed "old" / "new".
type MigrationReport struct {
	Op    string         `json:"op"` // "read" or "write"
	Stage MigrationStage `json:"stage"`
	// Consistent is set for shadow / live reads where both sides succeeded: did they agree?
	Consistent *bool             `json:"consistent,omitempty"`
	Errors     map[string]string `json:"errors"`
	LatencyMS  map[string]int64  `json:"latency_ms"`
}

// MigrationOps are the two sides of a migration.
type MigrationOps[R, W any] struct {
	ReadOld, ReadNew   func(context.Context) (R, error)
	WriteOld, WriteNew func(context.Context) (W, error)
	// Compare is the equality for shadow / live reads. Default: JSON equality.
	Compare func(oldValue, newValue R) bool
	// OnReport is called once per operation, e.g. to capture a `$migration_op` event.
	OnReport func(MigrationReport)
}

type migrationPlan struct {
	auth string // "old" or "new"
	both bool
}

var (
	migrationRead = map[MigrationStage]migrationPlan{
		MigrationOff: {"old", false}, MigrationDualwrite: {"old", false}, MigrationShadow: {"old", true},
		MigrationLive: {"new", true}, MigrationRampdown: {"new", false}, MigrationComplete: {"new", false},
	}
	migrationWrite = map[MigrationStage]migrationPlan{
		MigrationOff: {"old", false}, MigrationDualwrite: {"old", true}, MigrationShadow: {"old", true},
		MigrationLive: {"new", true}, MigrationRampdown: {"new", true}, MigrationComplete: {"new", false},
	}
)

func planFor(table map[MigrationStage]migrationPlan, stage MigrationStage) migrationPlan {
	if p, ok := table[stage]; ok {
		return p
	}
	return table[MigrationOff]
}

func otherSide(s string) string {
	if s == "old" {
		return "new"
	}
	return "old"
}

type sideResult[T any] struct {
	v   T
	err error
	ms  int64
}

func runSide[T any](ctx context.Context, f func(context.Context) (T, error)) sideResult[T] {
	t := time.Now()
	v, err := f(ctx)
	return sideResult[T]{v, err, time.Since(t).Milliseconds()}
}

func (r *MigrationReport) record(side string, err error, ms int64) {
	if err != nil {
		r.Errors[side] = err.Error()
	}
	r.LatencyMS[side] = ms
}

func pick[T any](side string, oldF, newF func(context.Context) (T, error)) func(context.Context) (T, error) {
	if side == "old" {
		return oldF
	}
	return newF
}

// MigrateRead reads through the stage: the authoritative side's result (or error) is returned;
// shadow / live also read the other side concurrently and compare.
func MigrateRead[R, W any](ctx context.Context, stage MigrationStage, ops MigrationOps[R, W]) (R, error) {
	plan := planFor(migrationRead, stage)
	rep := MigrationReport{Op: "read", Stage: stage, Errors: map[string]string{}, LatencyMS: map[string]int64{}}
	var shadow chan sideResult[R]
	if plan.both {
		shadow = make(chan sideResult[R], 1)
		go func() { shadow <- runSide(ctx, pick(otherSide(plan.auth), ops.ReadOld, ops.ReadNew)) }()
	}
	auth := runSide(ctx, pick(plan.auth, ops.ReadOld, ops.ReadNew))
	rep.record(plan.auth, auth.err, auth.ms)
	if shadow != nil {
		sh := <-shadow
		rep.record(otherSide(plan.auth), sh.err, sh.ms)
		if auth.err == nil && sh.err == nil {
			o, n := auth.v, sh.v
			if plan.auth == "new" {
				o, n = n, o
			}
			eq := jsonEqual(o, n)
			if ops.Compare != nil {
				eq = ops.Compare(o, n)
			}
			rep.Consistent = &eq
		}
	}
	if ops.OnReport != nil {
		ops.OnReport(rep)
	}
	return auth.v, auth.err
}

// MigrateWrite writes through the stage: the authoritative side first; the other side only after it
// succeeded (its failure is reported, not returned).
func MigrateWrite[R, W any](ctx context.Context, stage MigrationStage, ops MigrationOps[R, W]) (W, error) {
	plan := planFor(migrationWrite, stage)
	rep := MigrationReport{Op: "write", Stage: stage, Errors: map[string]string{}, LatencyMS: map[string]int64{}}
	auth := runSide(ctx, pick(plan.auth, ops.WriteOld, ops.WriteNew))
	rep.record(plan.auth, auth.err, auth.ms)
	if auth.err == nil && plan.both {
		sh := runSide(ctx, pick(otherSide(plan.auth), ops.WriteOld, ops.WriteNew))
		rep.record(otherSide(plan.auth), sh.err, sh.ms)
	}
	if ops.OnReport != nil {
		ops.OnReport(rep)
	}
	return auth.v, auth.err
}

func jsonEqual(a, b any) bool {
	x, err1 := json.Marshal(a)
	y, err2 := json.Marshal(b)
	return err1 == nil && err2 == nil && bytes.Equal(x, y)
}

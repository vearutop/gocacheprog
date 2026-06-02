package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type entry struct {
	Kind    string
	Payload string
	Package string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}
}

func run() error {
	kindFlag := flag.String("kind", "all", "HASH kind to compare, or 'all'")
	focusFlag := flag.String("focus", "inputs", "focus preset: all or inputs")
	moduleRootFlag := flag.String("module-root", "", "module root for actionable summaries, defaults to cwd")
	limitFlag := flag.Int("limit", 200, "max changed lines to print")
	verboseFlag := flag.Bool("verbose", false, "show full summary details")
	flag.Parse()

	var err error

	args := flag.Args()
	if len(args) < 1 || len(args) > 2 {
		return errors.New("usage: gocachehashdiff [flags] <log-a> [log-b]")
	}

	moduleRoot := *moduleRootFlag
	if moduleRoot == "" {
		moduleRoot, err = os.Getwd()
		if err != nil {
			return fmt.Errorf("get cwd: %w", err)
		}
	}
	moduleRoot, err = filepath.Abs(moduleRoot)
	if err != nil {
		return fmt.Errorf("abs module root: %w", err)
	}

	a, err := parseEntries(args[0], *kindFlag, *focusFlag)
	if err != nil {
		return fmt.Errorf("parse %s: %w", args[0], err)
	}

	if len(args) == 1 {
		printSummary(args[0], moduleRoot, *kindFlag, *focusFlag, a, *verboseFlag)
		return nil
	}

	b, err := parseEntries(args[1], *kindFlag, *focusFlag)
	if err != nil {
		return fmt.Errorf("parse %s: %w", args[1], err)
	}

	printDiff(args[0], args[1], *kindFlag, *focusFlag, moduleRoot, a, b, *limitFlag)
	return nil
}

func parseEntries(path, wantKind, focus string) ([]entry, error) {
	f, err := os.Open(path) //nolint:gosec // this CLI intentionally reads the user-specified log path.
	if err != nil {
		return nil, err
	}
	defer func() {
		if closeErr := f.Close(); closeErr != nil {
			fmt.Fprintf(os.Stderr, "close %s: %s\n", path, closeErr.Error())
		}
	}()

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 128*1024), 16*1024*1024)

	var res []entry
	for sc.Scan() {
		line := sc.Text()

		e, ok := parseEntry(line)
		if !ok {
			continue
		}

		if !allowByFocus(e.Kind, focus) {
			continue
		}
		if wantKind != "all" && e.Kind != wantKind {
			continue
		}

		res = append(res, e)
	}

	if err := sc.Err(); err != nil {
		return nil, err
	}

	return res, nil
}

func parseEntry(line string) (entry, bool) {
	if e, ok := parseTestCacheEntry(line); ok {
		return e, true
	}

	if !strings.HasPrefix(line, "HASH[") || !strings.Contains(line, "]: ") {
		return entry{}, false
	}

	closeIdx := strings.Index(line, "]")
	if closeIdx < 0 {
		return entry{}, false
	}

	kind := line[len("HASH["):closeIdx]
	prefix := "HASH[" + kind + "]: "
	if !strings.HasPrefix(line, prefix) {
		return entry{}, false
	}

	payload := strings.TrimPrefix(line, prefix)
	if unquoted, err := strconv.Unquote(payload); err == nil {
		payload = strings.TrimSuffix(unquoted, "\n")
	}

	return entry{Kind: kind, Payload: normalizePayload(kind, payload)}, true
}

func parseTestCacheEntry(line string) (entry, bool) {
	if !strings.HasPrefix(line, "testcache: ") {
		return entry{}, false
	}

	rest := strings.TrimPrefix(line, "testcache: ")
	pkg, payload, ok := strings.Cut(rest, ": ")
	if !ok || pkg == "" || payload == "" {
		return entry{}, false
	}

	return entry{
		Kind:    "testcache",
		Package: pkg,
		Payload: normalizePayload("testcache", payload),
	}, true
}

func normalizePayload(kind, payload string) string {
	payload = strings.TrimSpace(payload)

	switch kind {
	case "open", "stat":
		payload = scrubGoBuildDir(payload)
	case "getenv":
		payload = scrubTmpDir(payload)
	case "testInputs":
		payload = scrubGoBuildDir(payload)
		payload = scrubTmpDir(payload)
	case "testcache":
		payload = scrubGoBuildDir(payload)
		payload = scrubTmpDir(payload)
	}

	return payload
}

func scrubGoBuildDir(s string) string {
	s = strings.ReplaceAll(s, "/var/folders/", "/TMPROOT/")
	s = strings.ReplaceAll(s, "/tmp/go-build", "/TMPROOT/go-build")

	if i := strings.Index(s, "go-build"); i >= 0 {
		j := i + len("go-build")
		for j < len(s) && s[j] >= '0' && s[j] <= '9' {
			j++
		}
		s = s[:i] + "go-build<id>" + s[j:]
	}

	return s
}

func scrubTmpDir(s string) string {
	if strings.Contains(s, "/var/folders/") {
		return "/TMPROOT/"
	}

	return s
}

func allowByFocus(kind, focus string) bool {
	switch focus {
	case "", "all":
		return true
	case "inputs":
		return kind == "moduleIndex" ||
			kind == "open" ||
			kind == "stat" ||
			kind == "testInputs" ||
			kind == "getenv" ||
			kind == "testcache" ||
			kind == "testResult"
	default:
		return true
	}
}

func printSummary(path, moduleRoot, kind, focus string, entries []entry, verbose bool) {
	fmt.Printf("log: %s\n", path)
	fmt.Printf("kind: %s\n", kind)
	fmt.Printf("focus: %s\n", focus)
	fmt.Printf("lines: %d\n", len(entries))

	fmt.Println("\nActionable summary:")
	printSingleLogModuleIndexFiles(moduleRoot, entries)
	printSingleLogTestInputStats(moduleRoot, entries)
	printSingleLogTestCache(moduleRoot, entries)
	printSingleLogEnv(entries)

	if !verbose {
		return
	}

	counts := map[string]int{}
	for _, e := range entries {
		counts[e.Kind]++
	}

	fmt.Println("\nKinds:")
	for _, row := range sortedCountMap(counts) {
		fmt.Printf("%6d %s\n", row.Count, row.Key)
	}

	fmt.Println("\nSample:")
	for _, e := range entries[:minInt(20, len(entries))] {
		fmt.Printf("[%s] %s\n", e.Kind, e.Payload)
	}
}

func printSingleLogModuleIndexFiles(moduleRoot string, entries []entry) {
	var files []string
	for _, e := range entries {
		if e.Kind != "moduleIndex" || !strings.HasPrefix(e.Payload, "file ") {
			continue
		}
		p, ok := parseModuleIndexFile(e.Payload)
		if !ok {
			continue
		}
		if !isRepoLocalPath(p, moduleRoot) {
			continue
		}
		files = append(files, p)
	}
	files = uniqSorted(files)
	if len(files) == 0 {
		fmt.Println("- `moduleIndex`: no repo-local file inputs")
		return
	}
	fmt.Printf("- `moduleIndex`: %d repo-local file inputs\n", len(files))
	for _, p := range files[:minInt(10, len(files))] {
		fmt.Printf("  - %s\n", p)
	}
}

func printSingleLogTestInputStats(moduleRoot string, entries []entry) {
	var paths []string
	for _, e := range entries {
		if e.Kind != "testInputs" {
			continue
		}
		p, _, ok := parseTestInputStat(e.Payload)
		if !ok {
			continue
		}
		if !isRepoLocalPath(p, moduleRoot) {
			continue
		}
		paths = append(paths, p)
	}
	paths = uniqSorted(paths)
	if len(paths) == 0 {
		fmt.Println("- `testInputs`: no repo-local stat inputs")
		return
	}
	fmt.Printf("- `testInputs`: %d repo-local stat inputs\n", len(paths))
	for _, p := range paths[:minInt(10, len(paths))] {
		fmt.Printf("  - %s\n", p)
	}
}

func printSingleLogEnv(entries []entry) {
	var found []string
	if countPayloadPrefix(entries, "testInputs", "env GODEBUG") > 0 || countPayloadPrefix(entries, "getenv", "gocachehash=1") > 0 {
		found = append(found, "GODEBUG")
	}
	if countPayloadPrefix(entries, "testInputs", "env GOTMPDIR") > 0 || countPayloadPrefix(entries, "getenv", "/TMPROOT/") > 0 {
		found = append(found, "GOTMPDIR")
	}
	if countPayloadPrefix(entries, "testInputs", "env TMPDIR") > 0 || countPayloadPrefix(entries, "getenv", "/TMPROOT/") > 0 {
		found = append(found, "TMPDIR")
	}
	if len(found) == 0 {
		fmt.Println("- `env`: no interesting env signals")
		return
	}
	fmt.Printf("- `env`: %s\n", strings.Join(found, ", "))
}

func printSingleLogTestCache(moduleRoot string, entries []entry) {
	byPkg := collectTestCacheReasons(moduleRoot, entries)
	if len(byPkg) == 0 {
		fmt.Println("- `testcache`: no actionable package miss reasons")
		return
	}

	pkgs := sortedTestCachePackages(byPkg)
	fmt.Printf("- `testcache`: %d package(s) with actionable miss reasons\n", len(pkgs))
	for _, pkg := range pkgs[:minInt(5, len(pkgs))] {
		fmt.Printf("  - %s\n", pkg)
		reasons := sortedReasonKeys(byPkg[pkg])
		for _, reason := range reasons[:minInt(3, len(reasons))] {
			fmt.Printf("    * %s\n", reason)
		}
	}
}

func printDiff(aName, bName, kind, focus, moduleRoot string, a, b []entry, limit int) {
	am := countEntries(a)
	bm := countEntries(b)

	keysMap := map[string]struct{}{}
	for k := range am {
		keysMap[k] = struct{}{}
	}
	for k := range bm {
		keysMap[k] = struct{}{}
	}

	keys := make([]string, 0, len(keysMap))
	for k := range keysMap {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	fmt.Printf("compare kind=%s focus=%s\n", kind, focus)
	fmt.Printf("a: %s\n", aName)
	fmt.Printf("b: %s\n", bName)

	printActionableSummary(moduleRoot, a, b)

	kindChanges := map[string]int{}
	printed := 0
	for _, k := range keys {
		ac := am[k]
		bc := bm[k]
		if ac == bc {
			continue
		}

		kind, payload, _ := strings.Cut(k, "\x00")
		diff := ac - bc
		if diff < 0 {
			diff = -diff
		}
		kindChanges[kind] += diff

		if printed >= limit {
			continue
		}
		printed++

		switch {
		case ac == 0:
			fmt.Printf("\nADDED [%s] x%d\n  + %s\n", kind, bc, payload)
		case bc == 0:
			fmt.Printf("\nREMOVED [%s] x%d\n  - %s\n", kind, ac, payload)
		default:
			fmt.Printf("\nCOUNT CHANGED [%s] %d -> %d\n  = %s\n", kind, ac, bc, payload)
		}
	}

	if len(kindChanges) == 0 {
		fmt.Println("\nno relevant changes")
		return
	}

	fmt.Println("\nChanged by kind:")
	for _, row := range sortedCountMap(kindChanges) {
		fmt.Printf("%6d %s\n", row.Count, row.Key)
	}

	if printed >= limit {
		fmt.Printf("\noutput truncated at %d changed lines\n", limit)
	}
}

func printActionableSummary(moduleRoot string, a, b []entry) {
	fmt.Println("\nActionable summary:")

	printModuleIndexFiles(a, b)
	printTestInputStatChanges(a, b)
	printTestCacheChanges(moduleRoot, a, b)
	printEnvChanges(a, b)
}

func printModuleIndexFiles(a, b []entry) {
	am := map[string]int{}
	bm := map[string]int{}

	for _, e := range a {
		if e.Kind != "moduleIndex" || !strings.HasPrefix(e.Payload, "file ") {
			continue
		}
		if p, ok := parseModuleIndexFile(e.Payload); ok {
			am[p]++
		}
	}
	for _, e := range b {
		if e.Kind != "moduleIndex" || !strings.HasPrefix(e.Payload, "file ") {
			continue
		}
		if p, ok := parseModuleIndexFile(e.Payload); ok {
			bm[p]++
		}
	}

	var changed []string
	for p := range am {
		if am[p] != bm[p] {
			changed = append(changed, p)
		}
	}
	for p := range bm {
		if _, ok := am[p]; !ok {
			changed = append(changed, p)
		}
	}
	sort.Strings(changed)

	if len(changed) == 0 {
		fmt.Println("- `moduleIndex`: no file-list changes")
		return
	}

	fmt.Printf("- `moduleIndex`: %d file-list changes\n", len(changed))
	for _, p := range changed[:minInt(10, len(changed))] {
		fmt.Printf("  - %s\n", p)
	}
}

func parseModuleIndexFile(payload string) (string, bool) {
	rest := strings.TrimPrefix(payload, "file ")
	parts := strings.SplitN(rest, " ", 2)
	if len(parts) < 2 {
		return "", false
	}
	return parts[0], true
}

func printTestInputStatChanges(a, b []entry) {
	am := map[string]map[string]struct{}{}
	bm := map[string]map[string]struct{}{}

	for _, e := range a {
		if e.Kind != "testInputs" {
			continue
		}
		if p, h, ok := parseTestInputStat(e.Payload); ok {
			addSet(am, p, h)
		}
	}
	for _, e := range b {
		if e.Kind != "testInputs" {
			continue
		}
		if p, h, ok := parseTestInputStat(e.Payload); ok {
			addSet(bm, p, h)
		}
	}

	var changed []string
	for p := range am {
		if !equalSet(am[p], bm[p]) {
			changed = append(changed, p)
		}
	}
	for p := range bm {
		if _, ok := am[p]; !ok {
			changed = append(changed, p)
		}
	}
	sort.Strings(changed)

	if len(changed) == 0 {
		fmt.Println("- `testInputs`: no file stat input changes")
		return
	}

	fmt.Printf("- `testInputs`: %d file stat input changes\n", len(changed))
	for _, p := range changed[:minInt(10, len(changed))] {
		fmt.Printf("  - %s\n", p)
	}
}

func printTestCacheChanges(moduleRoot string, a, b []entry) {
	am := collectTestCacheReasons(moduleRoot, a)
	bm := collectTestCacheReasons(moduleRoot, b)

	changed := changedReasonPackages(am, bm)
	if len(changed) == 0 {
		fmt.Println("- `testcache`: no actionable package reason changes")
		return
	}

	fmt.Printf("- `testcache`: %d package reason changes\n", len(changed))
	for _, pkg := range changed[:minInt(5, len(changed))] {
		fmt.Printf("  - %s\n", pkg)

		added, removed := diffReasonSets(am[pkg], bm[pkg])
		for _, reason := range added[:minInt(2, len(added))] {
			fmt.Printf("    + %s\n", reason)
		}
		for _, reason := range removed[:minInt(2, len(removed))] {
			fmt.Printf("    - %s\n", reason)
		}
	}
}

func parseTestInputStat(payload string) (path string, hash string, ok bool) {
	if !strings.HasPrefix(payload, "stat ") {
		return "", "", false
	}
	rest := strings.TrimPrefix(payload, "stat ")
	last := strings.LastIndexByte(rest, ' ')
	if last <= 0 {
		return "", "", false
	}
	return rest[:last], rest[last+1:], true
}

func parseTestCacheReason(moduleRoot string, e entry) (string, bool) {
	if e.Kind != "testcache" {
		return "", false
	}

	if strings.HasPrefix(e.Payload, "input list not found:") {
		return "input list not found", true
	}

	const prefix = "input file "
	const suffix = ": file used as input is too new"
	if strings.HasPrefix(e.Payload, prefix) && strings.HasSuffix(e.Payload, suffix) {
		path := strings.TrimSuffix(strings.TrimPrefix(e.Payload, prefix), suffix)
		if isRepoLocalPath(path, moduleRoot) {
			if rel, err := filepath.Rel(moduleRoot, path); err == nil && rel != "." && !strings.HasPrefix(rel, "..") {
				path = rel
			}
		}
		return "input file too new: " + path, true
	}

	return "", false
}

func printEnvChanges(a, b []entry) {
	interesting := []string{"env GODEBUG", "env GOTMPDIR", "env TMPDIR"}
	var changed []string
	for _, prefix := range interesting {
		if countPayloadPrefix(a, "testInputs", prefix) != countPayloadPrefix(b, "testInputs", prefix) ||
			countPayloadPrefix(a, "getenv", strings.TrimPrefix(prefix, "env ")) != countPayloadPrefix(b, "getenv", strings.TrimPrefix(prefix, "env ")) {
			changed = append(changed, prefix)
		}
	}

	if len(changed) == 0 {
		fmt.Println("- `env`: no interesting env churn")
		return
	}

	fmt.Println("- `env`: changed signals")
	for _, c := range changed {
		fmt.Printf("  - %s\n", c)
	}
}

func countPayloadPrefix(entries []entry, kind, prefix string) int {
	n := 0
	for _, e := range entries {
		if e.Kind == kind && strings.HasPrefix(e.Payload, prefix) {
			n++
		}
	}
	return n
}

func collectTestCacheReasons(moduleRoot string, entries []entry) map[string]map[string]struct{} {
	byPkg := map[string]map[string]struct{}{}
	for _, e := range entries {
		reason, ok := parseTestCacheReason(moduleRoot, e)
		if !ok {
			continue
		}
		addSet(byPkg, e.Package, reason)
	}

	return byPkg
}

func sortedReasonKeys(set map[string]struct{}) []string {
	if len(set) == 0 {
		return nil
	}
	keys := make([]string, 0, len(set))
	for k := range set {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func sortedTestCachePackages(m map[string]map[string]struct{}) []string {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func changedReasonPackages(a, b map[string]map[string]struct{}) []string {
	keys := map[string]struct{}{}
	for k := range a {
		keys[k] = struct{}{}
	}
	for k := range b {
		keys[k] = struct{}{}
	}

	var changed []string
	for k := range keys {
		if !equalSet(a[k], b[k]) {
			changed = append(changed, k)
		}
	}
	sort.Strings(changed)
	return changed
}

func diffReasonSets(a, b map[string]struct{}) (added []string, removed []string) {
	for k := range b {
		if _, ok := a[k]; !ok {
			added = append(added, k)
		}
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			removed = append(removed, k)
		}
	}
	sort.Strings(added)
	sort.Strings(removed)
	return added, removed
}

func isRepoLocalPath(path, moduleRoot string) bool {
	if moduleRoot == "" {
		return false
	}
	if filepath.IsAbs(path) {
		rel, err := filepath.Rel(moduleRoot, path)
		return err == nil && rel != "." && !strings.HasPrefix(rel, "..")
	}
	return true
}

func uniqSorted(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	sort.Strings(values)
	res := values[:1]
	for _, v := range values[1:] {
		if v != res[len(res)-1] {
			res = append(res, v)
		}
	}
	return res
}

func addSet(m map[string]map[string]struct{}, key, value string) {
	s, ok := m[key]
	if !ok {
		s = map[string]struct{}{}
		m[key] = s
	}
	s[value] = struct{}{}
}

func equalSet(a, b map[string]struct{}) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

func countEntries(entries []entry) map[string]int {
	res := make(map[string]int, len(entries))
	for _, e := range entries {
		key := e.Kind + "\x00" + e.Payload
		res[key]++
	}
	return res
}

type countRow struct {
	Key   string
	Count int
}

func sortedCountMap(m map[string]int) []countRow {
	rows := make([]countRow, 0, len(m))
	for k, v := range m {
		rows = append(rows, countRow{Key: k, Count: v})
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Count == rows[j].Count {
			return rows[i].Key < rows[j].Key
		}
		return rows[i].Count > rows[j].Count
	})
	return rows
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

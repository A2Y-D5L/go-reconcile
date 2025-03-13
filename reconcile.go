package reconcile

import (
   "fmt"
   "hash/fnv"
   "reflect"
   "sort"
   "strings"
   "sync"

   "iter"
   "unique"

   metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// structFieldConcurrencyThreshold is the minimum number of struct fields
// to warrant using concurrent goroutine comparisons.
const structFieldConcurrencyThreshold = 3

// FieldDiff represents a difference at a given field.
type FieldDiff struct {
   Field    string      `json:"field"`
   Observed interface{} `json:"observed"`
   Desired  interface{} `json:"desired"`
}

// ReconcileCondition represents a reconciliation condition.
type ReconcileCondition struct {
   Type               string      `json:"type"`
   Status             string      `json:"status"` // "true", "false", or "unknown"
   LastTransitionTime metav1.Time `json:"lastTransitionTime"`
   Message            string      `json:"message"`
   Details            []FieldDiff `json:"details,omitempty"`
}

// GenericStatus is the output of a generalized reconcile function.
type GenericStatus struct {
   Conditions         []ReconcileCondition `json:"conditions"`
   ObservedGeneration int64                `json:"observedGeneration"`
   Ready              bool                 `json:"ready"`
}

//
// Global cache for canonical (unique) values.
// The uniqueStore is protected by a sync.RWMutex for concurrency safety.
//
var (
   uniqueStore   = map[string]unique.Handle[any]{}
   uniqueStoreMu sync.RWMutex
)

// hashKey returns an efficient hash string for v using FNV-1a.
// For basic types we write "%T:%v", and for composite types "%T:%#v".
func hashKey(v interface{}) string {
   h := fnv.New64a()
   rv := reflect.ValueOf(v)
   switch rv.Kind() {
   case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
       reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
       reflect.Float32, reflect.Float64, reflect.String:
       _, _ = fmt.Fprintf(h, "%T:%v", v, v)
   default:
       _, _ = fmt.Fprintf(h, "%T:%#v", v, v)
   }
   return fmt.Sprintf("%x", h.Sum64())
}

// internValue returns a canonical handle for v.
// It uses an efficient hash function for key generation.
func internValue(v interface{}) unique.Handle[any] {
   key := hashKey(v)

   uniqueStoreMu.RLock()
   handle, exists := uniqueStore[key]
   uniqueStoreMu.RUnlock()
   if exists {
       return handle
   }

   handle = unique.Make(v)
   uniqueStoreMu.Lock()
   uniqueStore[key] = handle
   uniqueStoreMu.Unlock()
   return handle
}

// ReconcileGeneric compares two arbitrary objects and produces a GenericStatus.
// In case of an unexpected condition (such as incompatible types),
// the "synced" condition is marked false with details explaining the issue.
func ReconcileGeneric(observed, desired any) GenericStatus {
   var status GenericStatus
   now := metav1.Now()

   ov := reflect.ValueOf(observed)
   dv := reflect.ValueOf(desired)

   // If both values are valid and their types differ, report an error condition.
   if ov.IsValid() && dv.IsValid() && ov.Type() != dv.Type() {
       diff := FieldDiff{
           Field:    "",
           Observed: fmt.Sprintf("Type %s", ov.Type().String()),
           Desired:  fmt.Sprintf("Type %s", dv.Type().String()),
       }
       upToDateCondition := ReconcileCondition{
           Type:               "upToDate",
           LastTransitionTime: now,
           Status:             "false",
           Message:            "Incompatible types between observed and desired values.",
           Details:            []FieldDiff{diff},
       }
       syncedCondition := ReconcileCondition{
           Type:               "synced",
           LastTransitionTime: now,
           Status:             "false",
           Message:            "Reconciliation failed due to incompatible types.",
           Details:            []FieldDiff{diff},
       }
       status.Conditions = []ReconcileCondition{upToDateCondition, syncedCondition}
       status.ObservedGeneration = extractObservedGeneration(observed)
       status.Ready = false
       return status
   }

   // Otherwise, perform a deep field comparison.
   fieldDiffs := collectFieldDiffs(ov, dv, "")

   upToDateCondition := ReconcileCondition{
       Type:               "upToDate",
       LastTransitionTime: now,
       Status:             "true",
       Message:            "The observed state matches the desired state.",
   }

   if len(fieldDiffs) > 0 {
       upToDateCondition.Status = "false"
       upToDateCondition.Message = "The observed state does not match the desired state."
       upToDateCondition.Details = fieldDiffs
   }

   syncedCondition := ReconcileCondition{
       Type:               "synced",
       LastTransitionTime: now,
       Status:             "true",
       Message:            "Reconciliation completed successfully.",
   }

   status.Conditions = []ReconcileCondition{upToDateCondition, syncedCondition}
   status.ObservedGeneration = extractObservedGeneration(observed)
   status.Ready = (upToDateCondition.Status == "true" && syncedCondition.Status == "true")
   return status
}

// collectFieldDiffs recursively compares observed and desired values,
// returning a slice of FieldDiff. For basic types the unique handle shortcut
// is used; for composite types, a deep (field-by-field or element-by-element)
// comparison is performed.
func collectFieldDiffs(observed, desired reflect.Value, path string) []FieldDiff {
   var diffs []FieldDiff

   // Handle invalid (nil) values.
   if !observed.IsValid() || !desired.IsValid() {
       diffs = append(diffs, FieldDiff{
           Field:    path,
           Observed: valueInterface(observed),
           Desired:  valueInterface(desired),
       })
       return diffs
   }

   // Determine the kind and use a shortcut for basic types.
   kind := observed.Kind()
   switch kind {
   case reflect.Bool, reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
       reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
       reflect.Float32, reflect.Float64, reflect.String:
       obsInterface := observed.Interface()
       desInterface := desired.Interface()
       if internValue(obsInterface) != internValue(desInterface) {
           diffs = append(diffs, FieldDiff{
               Field:    path,
               Observed: obsInterface,
               Desired:  desInterface,
           })
       }
       return diffs
   }

   // Special handling for pointers.
   if observed.Kind() == reflect.Ptr && desired.Kind() == reflect.Ptr {
       if observed.IsNil() || desired.IsNil() {
           diffs = append(diffs, FieldDiff{
               Field:    path,
               Observed: valueInterface(observed),
               Desired:  valueInterface(desired),
           })
           return diffs
       }
       return collectFieldDiffs(observed.Elem(), desired.Elem(), path)
   }

   // Handle composite types.
   switch kind {
   case reflect.Struct:
       numFields := observed.NumField()
       // Use concurrency if the number of fields exceeds the threshold.
       if numFields > structFieldConcurrencyThreshold {
           var wg sync.WaitGroup
           var mu sync.Mutex
           for i := 0; i < numFields; i++ {
               field := observed.Type().Field(i)
               // Skip unexported fields.
               if field.PkgPath != "" {
                   continue
               }
               wg.Add(1)
               go func(i int, fieldName string) {
                   defer wg.Done()
                   fieldPath := fmt.Sprintf("%s.%s", path, fieldName)
                   subDiffs := collectFieldDiffs(observed.Field(i), desired.Field(i), fieldPath)
                   mu.Lock()
                   diffs = append(diffs, subDiffs...)
                   mu.Unlock()
               }(i, field.Name)
           }
           wg.Wait()
       } else {
           for i := 0; i < numFields; i++ {
               field := observed.Type().Field(i)
               if field.PkgPath != "" {
                   continue
               }
               fieldPath := fmt.Sprintf("%s.%s", path, field.Name)
               subDiffs := collectFieldDiffs(observed.Field(i), desired.Field(i), fieldPath)
               diffs = append(diffs, subDiffs...)
           }
       }
   case reflect.Map:
       // Build a lookup of observed keys to their corresponding value.
       obsMap := make(map[interface{}]interface{})
       for _, key := range observed.MapKeys() {
           obsMap[key.Interface()] = observed.MapIndex(key).Interface()
       }
       // Iterate over the desired map.
       for _, key := range desired.MapKeys() {
           newPath := fmt.Sprintf("%s[%v]", path, key.Interface())
           desVal := desired.MapIndex(key).Interface()
           if obsVal, exists := obsMap[key.Interface()]; exists {
               diffs = append(diffs, collectFieldDiffs(reflect.ValueOf(obsVal), reflect.ValueOf(desVal), newPath)...)
               delete(obsMap, key.Interface())
           } else {
               diffs = append(diffs, FieldDiff{
                   Field:    newPath,
                   Observed: nil,
                   Desired:  desVal,
               })
           }
       }
       // Any keys left in obsMap are present only in observed.
       for k, obsVal := range obsMap {
           newPath := fmt.Sprintf("%s[%v]", path, k)
           diffs = append(diffs, FieldDiff{
               Field:    newPath,
               Observed: obsVal,
               Desired:  nil,
           })
       }
   case reflect.Slice, reflect.Array:
       // Convert the slices/arrays to []any.
       observedSlice := reflectSliceToAnySlice(observed)
       desiredSlice := reflectSliceToAnySlice(desired)
       // Sort the slices for deterministic comparison.
       observedSlice = sortedSlice(observedSlice)
       desiredSlice = sortedSlice(desiredSlice)
       maxLen := len(observedSlice)
       if len(desiredSlice) > maxLen {
           maxLen = len(desiredSlice)
       }
       for i := 0; i < maxLen; i++ {
           newPath := fmt.Sprintf("%s[%d]", path, i)
           var obsElem, desElem any
           if i < len(observedSlice) {
               obsElem = observedSlice[i]
           }
           if i < len(desiredSlice) {
               desElem = desiredSlice[i]
           }
           diffs = append(diffs, collectFieldDiffs(reflect.ValueOf(obsElem), reflect.ValueOf(desElem), newPath)...)
       }
   }

   // Ensure deterministic ordering by sorting the diffs by Field path.
   sort.Slice(diffs, func(i, j int) bool {
       return strings.Compare(diffs[i].Field, diffs[j].Field) < 0
   })
   return diffs
}

// extractObservedGeneration efficiently retrieves the ObservedGeneration field.
// It returns -1 if no such field is found.
func extractObservedGeneration(observed any) int64 {
   v := reflect.Indirect(reflect.ValueOf(observed))
   if v.Kind() != reflect.Struct {
       return -1
   }

   if statusField := v.FieldByName("Status"); statusField.IsValid() && statusField.Kind() == reflect.Struct {
       if observedGenField := statusField.FieldByName("ObservedGeneration"); observedGenField.IsValid() && observedGenField.CanInt() {
           return observedGenField.Int()
       }
   }
   return -1
}

// valueInterface safely returns the interface of a reflect.Value,
// or nil if the value is not valid.
func valueInterface(val reflect.Value) interface{} {
   if !val.IsValid() {
       return nil
   }
   return val.Interface()
}

// reflectSliceToAnySlice converts a reflect.Value of kind slice or array to a []any.
func reflectSliceToAnySlice(v reflect.Value) []any {
   n := v.Len()
   result := make([]any, n)
   for i := 0; i < n; i++ {
       result[i] = v.Index(i).Interface()
   }
   return result
}

// sortedSlice returns a sorted copy of s. Sorting is done based on the string representation.
func sortedSlice(s []any) []any {
   // Make a copy to avoid modifying the original slice.
   cpy := make([]any, len(s))
   copy(cpy, s)
   sort.Slice(cpy, func(i, j int) bool {
       return fmt.Sprintf("%v", cpy[i]) < fmt.Sprintf("%v", cpy[j])
   })
   return cpy
}

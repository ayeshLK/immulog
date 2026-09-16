// Copyright 2026 Ayesh Almeida
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/ayeshLK/immulog/api"
)

type metadataBootstrap struct {
	catalog             *Partition
	offsets             *Partition
	projection          *catalogProjection
	offsetsState        *offsetsProjection
	snapshotDiagnostics SnapshotDiagnostics
}

func bootstrapMetadata(rootPath string, fallback StoreID, limits StoreOptions) (*metadataBootstrap, error) {
	catalogDir := filepath.Join(rootPath, clusterMetadataDir)
	offsetsDir := filepath.Join(rootPath, consumerOffsetsDir)
	if err := fsMkdirAll(catalogDir, 0o755); err != nil {
		return nil, fmt.Errorf("create catalog directory: %w", err)
	}
	if err := syncDir(filepath.Dir(catalogDir)); err != nil {
		return nil, fmt.Errorf("sync system directory: %w", err)
	}
	systemConfig := systemPartitionConfig()
	options := systemConfig.options()
	if err := options.validateWriter(); err != nil {
		return nil, err
	}
	catalogFiles, err := discoverSegments(catalogDir)
	if err != nil {
		return nil, fmt.Errorf("inspect catalog log: %w", err)
	}
	if len(catalogFiles) == 0 {
		offsetFiles, err := existingSegments(offsetsDir)
		if err != nil {
			return nil, fmt.Errorf("inspect offsets log: %w", err)
		}
		if len(offsetFiles) != 0 {
			return nil, corrupt(errInvalidSegment, "offset history exists before StoreInitialized")
		}
		if err := rejectUninitializedData(rootPath); err != nil {
			return nil, err
		}
		storeID, err := newStoreID()
		if err != nil {
			return nil, err
		}
		catalog, err := openPartition(catalogDir, api.ClusterMetadataTopicID, 0, options, storeID)
		if err != nil {
			return nil, err
		}
		if err := fsMkdirAll(offsetsDir, 0o755); err != nil {
			_ = catalog.Close()
			return nil, fmt.Errorf("create offsets directory: %w", err)
		}
		if err := syncDir(filepath.Dir(offsetsDir)); err != nil {
			_ = catalog.Close()
			return nil, fmt.Errorf("sync offsets parent directory: %w", err)
		}
		offsets, err := openPartition(offsetsDir, api.ConsumerOffsetsTopicID, 0, options, storeID)
		if err != nil {
			_ = catalog.Close()
			return nil, err
		}
		if err := appendStoreInitialized(catalog, offsets, storeID, systemConfig, limits.MaxCatalogHistoryBytes); err != nil {
			_ = offsets.Close()
			_ = catalog.Close()
			return nil, err
		}
		projection := newCatalogProjection()
		if err := projection.replay(catalog); err != nil {
			_ = offsets.Close()
			_ = catalog.Close()
			return nil, err
		}
		if err := preflightTopicStorage(rootPath, projection); err != nil {
			_ = offsets.Close()
			_ = catalog.Close()
			return nil, err
		}
		if err := reconcileRetiredArtifacts(rootPath, projection); err != nil {
			_ = offsets.Close()
			_ = catalog.Close()
			return nil, err
		}
		offsetsState, err := replayOffsetsHistory(offsets, storeID, projection)
		if err != nil {
			_ = offsets.Close()
			_ = catalog.Close()
			return nil, err
		}
		if err := admitSystemHistory(catalog, offsets, limits); err != nil {
			_ = offsets.Close()
			_ = catalog.Close()
			return nil, err
		}
		diagnostics := validateOptionalSnapshots(storeID, catalog, offsets, projection, offsetsState)
		return &metadataBootstrap{catalog: catalog, offsets: offsets, projection: projection, offsetsState: offsetsState, snapshotDiagnostics: diagnostics}, nil
	}

	if err := preflightSystemLogDirectory(catalogDir, api.ClusterMetadataTopicID, 0, systemConfig); err != nil {
		return nil, fmt.Errorf("preflight catalog log: %w", err)
	}
	offsetFiles, err := existingSegments(offsetsDir)
	if err != nil {
		return nil, fmt.Errorf("inspect offsets bootstrap: %w", err)
	}
	if len(offsetFiles) != 0 {
		if err := preflightSystemLogDirectory(offsetsDir, api.ConsumerOffsetsTopicID, 0, systemConfig); err != nil {
			return nil, fmt.Errorf("preflight offsets log: %w", err)
		}
	}
	catalog, err := openPartition(catalogDir, api.ClusterMetadataTopicID, 0, options, fallback)
	if err != nil {
		return nil, err
	}
	first, err := catalog.Read(0, 1)
	if err != nil {
		_ = catalog.Close()
		return nil, err
	}
	if len(first) == 0 {
		if err := fsMkdirAll(offsetsDir, 0o755); err != nil {
			_ = catalog.Close()
			return nil, fmt.Errorf("create offsets directory: %w", err)
		}
		offsets, err := openPartition(offsetsDir, api.ConsumerOffsetsTopicID, 0, options, fallback)
		if err != nil {
			_ = catalog.Close()
			return nil, err
		}
		if len(offsetFiles) != 0 {
			end, err := offsets.EndOffset()
			if err != nil {
				_ = offsets.Close()
				_ = catalog.Close()
				return nil, err
			}
			if end != 0 {
				_ = offsets.Close()
				_ = catalog.Close()
				return nil, corrupt(errInvalidRecord, "offset history exists before StoreInitialized")
			}
		}
		storeID, err := newStoreID()
		if err != nil {
			_ = offsets.Close()
			_ = catalog.Close()
			return nil, err
		}
		if err := appendStoreInitialized(catalog, offsets, storeID, systemConfig, limits.MaxCatalogHistoryBytes); err != nil {
			_ = offsets.Close()
			_ = catalog.Close()
			return nil, err
		}
		projection := newCatalogProjection()
		if err := projection.replay(catalog); err != nil {
			_ = offsets.Close()
			_ = catalog.Close()
			return nil, err
		}
		if err := preflightTopicStorage(rootPath, projection); err != nil {
			_ = offsets.Close()
			_ = catalog.Close()
			return nil, err
		}
		if err := reconcileRetiredArtifacts(rootPath, projection); err != nil {
			_ = offsets.Close()
			_ = catalog.Close()
			return nil, err
		}
		offsetsState, err := replayOffsetsHistory(offsets, storeID, projection)
		if err != nil {
			_ = offsets.Close()
			_ = catalog.Close()
			return nil, err
		}
		if err := admitSystemHistory(catalog, offsets, limits); err != nil {
			_ = offsets.Close()
			_ = catalog.Close()
			return nil, err
		}
		diagnostics := validateOptionalSnapshots(storeID, catalog, offsets, projection, offsetsState)
		return &metadataBootstrap{catalog: catalog, offsets: offsets, projection: projection, offsetsState: offsetsState, snapshotDiagnostics: diagnostics}, nil
	}
	eventType, payload, err := decodeSystemEvent(first[0], api.ClusterMetadataTopicID)
	if err != nil {
		_ = catalog.Close()
		return nil, err
	}
	if eventType != EventStoreInitialized {
		_ = catalog.Close()
		return nil, corrupt(errInvalidRecord, "catalog does not begin with StoreInitialized")
	}
	if len(offsetFiles) == 0 {
		_ = catalog.Close()
		return nil, corrupt(errInvalidSegment, "initialized store is missing consumer-offsets history")
	}
	initialized, err := decodeStoreInitializedPayload(payload)
	if err != nil {
		_ = catalog.Close()
		return nil, err
	}
	if initialized.StoreID == (StoreID{}) {
		_ = catalog.Close()
		return nil, corrupt(errInvalidRecord, "catalog StoreID is zero")
	}
	catalog.storeID = initialized.StoreID
	catalog.options = initialized.CatalogConfig.options()
	rebindIndexes(catalog)
	if err := catalog.options.validateWriter(); err != nil {
		_ = catalog.Close()
		return nil, err
	}
	if catalog.segments[0].header.ID != initialized.CatalogInitial.ID || catalog.segments[0].headerHash != initialized.CatalogInitial.HeaderHash {
		_ = catalog.Close()
		return nil, corrupt(errInvalidSegment, "catalog initial anchor mismatch")
	}
	offsets, err := openPartition(offsetsDir, api.ConsumerOffsetsTopicID, 0, initialized.OffsetsConfig.options(), initialized.StoreID)
	if err != nil {
		_ = catalog.Close()
		return nil, err
	}
	offsets.storeID = initialized.StoreID
	offsets.options = initialized.OffsetsConfig.options()
	rebindIndexes(offsets)
	if offsets.segments[0].header.ID != initialized.OffsetsInitial.ID || offsets.segments[0].headerHash != initialized.OffsetsInitial.HeaderHash {
		_ = offsets.Close()
		_ = catalog.Close()
		return nil, corrupt(errInvalidSegment, "consumer-offsets initial anchor mismatch")
	}
	projection := newCatalogProjection()
	if err := projection.replay(catalog); err != nil {
		_ = offsets.Close()
		_ = catalog.Close()
		return nil, err
	}
	if projection.storeID != initialized.StoreID {
		_ = offsets.Close()
		_ = catalog.Close()
		return nil, corrupt(errInvalidRecord, "catalog StoreID projection mismatch")
	}
	if err := preflightTopicStorage(rootPath, projection); err != nil {
		_ = offsets.Close()
		_ = catalog.Close()
		return nil, err
	}
	if err := reconcileRetiredArtifacts(rootPath, projection); err != nil {
		_ = offsets.Close()
		_ = catalog.Close()
		return nil, err
	}
	offsetsState, err := replayOffsetsHistory(offsets, initialized.StoreID, projection)
	if err != nil {
		_ = offsets.Close()
		_ = catalog.Close()
		return nil, err
	}
	if err := admitSystemHistory(catalog, offsets, limits); err != nil {
		_ = offsets.Close()
		_ = catalog.Close()
		return nil, err
	}
	diagnostics := validateOptionalSnapshots(initialized.StoreID, catalog, offsets, projection, offsetsState)
	return &metadataBootstrap{catalog: catalog, offsets: offsets, projection: projection, offsetsState: offsetsState, snapshotDiagnostics: diagnostics}, nil
}

func appendStoreInitialized(catalog, offsets *Partition, storeID StoreID, config PartitionConfigV1, catalogLimit uint64) error {
	if catalog == nil || offsets == nil || len(catalog.segments) == 0 || len(offsets.segments) == 0 {
		return errors.New("system-log bootstrap partitions are unavailable")
	}
	payload, err := encodeStoreInitializedPayload(storeInitializedEvent{
		StoreID:        storeID,
		CatalogInitial: eventAnchor{ID: catalog.segments[0].header.ID, HeaderHash: catalog.segments[0].headerHash},
		OffsetsInitial: eventAnchor{ID: offsets.segments[0].header.ID, HeaderHash: offsets.segments[0].headerHash},
		CatalogConfig:  config,
		OffsetsConfig:  config,
	})
	if err != nil {
		return err
	}
	value, err := encodeSystemEvent(EventStoreInitialized, payload)
	if err != nil {
		return err
	}
	if err := systemEventAdmission(catalog, api.ClusterMetadataTopicID, 0, value, catalogLimit); err != nil {
		return err
	}
	_, err = catalog.AppendBatch(api.RecordBatch{
		Topic: api.ClusterMetadataTopicID, Partition: 0, BaseOffset: 0,
		Records: []api.Record{{Topic: api.ClusterMetadataTopicID, Partition: 0, Offset: 0, Value: value}},
	})
	return err
}

func admitSystemHistory(catalog, offsets *Partition, limits StoreOptions) error {
	if err := systemLogAdmission(catalog, 0, limits.MaxCatalogHistoryBytes); err != nil {
		return err
	}
	return systemLogAdmission(offsets, 0, limits.MaxOffsetsHistoryBytes)
}

func validateOffsetsHistory(offsets *Partition, storeID StoreID) error {
	end, err := offsets.EndOffset()
	if err != nil {
		return err
	}
	for offset := uint64(0); offset < end; {
		records, err := offsets.Read(offset, MaxBatchRecords)
		if err != nil {
			return err
		}
		if len(records) == 0 {
			return corrupt(errInvalidRecord, "offset history stopped before durable end")
		}
		for _, record := range records {
			if record.Offset != offset {
				return corrupt(errInvalidRecord, "offset history gap")
			}
			eventType, payload, err := decodeSystemEvent(record, api.ConsumerOffsetsTopicID)
			if err != nil {
				return err
			}
			if err := validateOffsetsEvent(eventType, payload, storeID); err != nil {
				return err
			}
			offset++
		}
	}
	return nil
}

func rejectUninitializedData(rootPath string) error {
	entries, err := fsReadDir(rootPath)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() == "LOCK" || entry.Name() == "system" {
			continue
		}
		return errors.Join(api.ErrCorruptLog, errors.New("uninitialized data directory contains unknown authoritative state"))
	}
	return nil
}

func newStoreID() (StoreID, error) {
	var id StoreID
	if _, err := rand.Read(id[:]); err != nil {
		return StoreID{}, fmt.Errorf("generate StoreID: %w", err)
	}
	if id == (StoreID{}) {
		id[15] = 1
	}
	return id, nil
}

func rebindIndexes(partition *Partition) {
	for _, segment := range partition.segments {
		loadOrBuildIndexes(segment, partition.storeID, partition.options.IndexStride)
	}
}

func existingSegments(dir string) ([]discoveredSegment, error) {
	if _, err := fsStat(dir); errors.Is(err, os.ErrNotExist) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return discoverSegments(dir)
}

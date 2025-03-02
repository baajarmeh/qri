// Package logbook records and syncs dataset histories. As users work on
// datasets, they build of a log of operations. Each operation is a record
// of an action taken, like creating a dataset, or unpublishing a version.
// Each of these operations is wrtten to a log attributed to the user that
// performed the action, and stored in the logbook under the namespace of that
// dataset. The current state of a user's log is derived from iterating over
// all operations to produce the current state.
package logbook

import (
	"context"
	"fmt"
	"io/ioutil"
	"strings"
	"sync"
	"time"

	golog "github.com/ipfs/go-log"
	crypto "github.com/libp2p/go-libp2p-core/crypto"
	"github.com/qri-io/dataset"
	"github.com/qri-io/qfs"
	"github.com/qri-io/qri/auth/key"
	"github.com/qri-io/qri/dsref"
	"github.com/qri-io/qri/event"
	"github.com/qri-io/qri/logbook/oplog"
	"github.com/qri-io/qri/profile"
	"github.com/qri-io/qri/transform/run"
)

var (
	// ErrNoLogbook indicates a logbook doesn't exist
	ErrNoLogbook = fmt.Errorf("logbook: does not exist")
	// ErrNotFound is a sentinel error for data not found in a logbook
	ErrNotFound = fmt.Errorf("logbook: not found")
	// ErrLogTooShort indicates a log is missing elements. Because logs are
	// append-only, passing a shorter log than the one on file is grounds
	// for rejection
	ErrLogTooShort = fmt.Errorf("logbook: log is too short")
	// ErrAccessDenied indicates insufficent privileges to perform a logbook
	// operation
	ErrAccessDenied = fmt.Errorf("access denied")

	// NewTimestamp generates the current unix nanosecond time.
	// This is mainly here for tests to override
	NewTimestamp = func() int64 { return time.Now().UnixNano() }

	log = golog.Logger("logbook")
)

const (
	// AuthorModel is the enum for an author model
	AuthorModel uint32 = iota
	// DatasetModel is the enum for a dataset model
	DatasetModel
	// BranchModel is the enum for a branch model
	BranchModel
	// CommitModel is the enum for a commit model
	CommitModel
	// PushModel is the enum for a push model
	PushModel
	// RunModel is the enum for transform execution
	RunModel
	// ACLModel is the enum for a acl model
	ACLModel
)

const (
	// DefaultBranchName is the default name all branch-level logbook data is read
	// from and written to. we currently don't present branches as a user-facing
	// feature in qri, but logbook supports them
	DefaultBranchName = "main"
	// runIDRelPrefix is a string prefix for op.Relations when recording commit ops
	// that have a non-empty Commit.RunID field. A commit operation that has a
	// related runID will have op.Relations = [...,"runID:run-uuid-string",...],
	// This prefix disambiguates from other types of identifiers
	runIDRelPrefix = "runID:"
)

// ModelString gets a unique string descriptor for an integral model identifier
func ModelString(m uint32) string {
	switch m {
	case AuthorModel:
		return "user"
	case DatasetModel:
		return "dataset"
	case BranchModel:
		return "branch"
	case CommitModel:
		return "commit"
	case PushModel:
		return "push"
	case ACLModel:
		return "acl"
	case RunModel:
		return "run"
	default:
		return ""
	}
}

// Book wraps a oplog.Book with a higher-order API specific to Qri
type Book struct {
	store oplog.Logstore

	pk         crypto.PrivKey
	authorID   string
	authorName string

	fsLocation string
	fs         qfs.Filesystem

	publisher event.Publisher
}

// NewBook creates a book with a user-provided logstore
func NewBook(pk crypto.PrivKey, store oplog.Logstore) *Book {
	return &Book{pk: pk, store: store}
}

// NewJournal initializes a logbook owned by a single author, reading any
// existing data at the given filesystem location.
// logbooks are encrypted at rest with the given private key
func NewJournal(pk crypto.PrivKey, username string, bus event.Bus, fs qfs.Filesystem, location string) (*Book, error) {
	ctx := context.Background()
	if pk == nil {
		return nil, fmt.Errorf("logbook: private key is required")
	}
	if fs == nil {
		return nil, fmt.Errorf("logbook: filesystem is required")
	}
	if location == "" {
		return nil, fmt.Errorf("logbook: location is required")
	}
	if bus == nil {
		return nil, fmt.Errorf("logbook: event.Bus is required")
	}

	book := &Book{
		store:      &oplog.Journal{},
		fs:         fs,
		pk:         pk,
		authorName: username,
		fsLocation: location,
		publisher:  bus,
	}

	if err := book.load(ctx); err != nil {
		if err == ErrNotFound {
			keyID, err := key.IDFromPrivKey(book.pk)
			if err != nil {
				return nil, err
			}
			err = book.initialize(ctx, keyID)
			return book, err
		}
		return nil, err
	}
	// else {
	// TODO (b5) verify username integrity on load
	// }

	return book, nil
}

// NewJournalOverwriteWithProfileID initializes a new logbook using the
// given profileID. Any existing logbook will be overwritten.
func NewJournalOverwriteWithProfileID(pk crypto.PrivKey, username string, bus event.Bus, fs qfs.Filesystem, location, profileID string) (*Book, error) {
	ctx := context.Background()
	if pk == nil {
		return nil, fmt.Errorf("logbook: private key is required")
	}
	if fs == nil {
		return nil, fmt.Errorf("logbook: filesystem is required")
	}
	if location == "" {
		return nil, fmt.Errorf("logbook: location is required")
	}
	if profileID == "" {
		return nil, fmt.Errorf("logbook: profileID is required")
	}
	if bus == nil {
		return nil, fmt.Errorf("logbook: event.Bus is required")
	}

	book := &Book{
		store:      &oplog.Journal{},
		fs:         fs,
		pk:         pk,
		authorName: username,
		fsLocation: location,
		publisher:  bus,
	}

	err := book.initialize(ctx, profileID)
	return book, err
}

func (book *Book) initialize(ctx context.Context, authorID string) error {
	// initialize author's log of user actions
	userActions := oplog.InitLog(oplog.Op{
		Type:      oplog.OpTypeInit,
		Model:     AuthorModel,
		Name:      book.Username(),
		AuthorID:  authorID,
		Timestamp: NewTimestamp(),
	})
	book.authorID = userActions.ID()

	if err := book.store.MergeLog(ctx, userActions); err != nil {
		return err
	}
	if al, ok := book.store.(oplog.AuthorLogstore); ok {
		al.SetID(ctx, book.authorID)
	}
	return book.save(ctx)
}

// ActivePeerID returns the in-use PeerID of the logbook author
// TODO (b5) - remove the need for this context by caching the active PeerID
// at key load / save / mutation points
func (book *Book) ActivePeerID(ctx context.Context) (id string, err error) {
	if book == nil {
		return "", ErrNoLogbook
	}

	lg, err := book.store.Get(ctx, book.authorID)
	if err != nil {
		panic(err)
	}
	return lg.Author(), nil
}

// Author returns this book's author
func (book *Book) Author() profile.Author {
	return book
}

// Username returns the human-readable name of the author
func (book *Book) Username() string {
	return book.authorName
}

// AuthorID returns the machine identifier for a name
func (book *Book) AuthorID() string {
	return book.authorID
}

// AuthorPubKey gives this book's author public key
func (book *Book) AuthorPubKey() crypto.PubKey {
	return book.pk.GetPublic()
}

// RenameAuthor marks a change in author name
func (book *Book) RenameAuthor() error {
	return fmt.Errorf("not finished")
}

// DeleteAuthor removes an author, used on teardown
func (book *Book) DeleteAuthor() error {
	return fmt.Errorf("not finished")
}

// ReplaceAll replaces the contents of the logbook with
// the provided log data
func (book *Book) ReplaceAll(ctx context.Context, lg *oplog.Log) error {
	err := book.store.ReplaceAll(ctx, lg)
	if err != nil {
		return err
	}
	return book.save(ctx)
}

// save writes the book to book.fsLocation
func (book *Book) save(ctx context.Context) (err error) {
	if al, ok := book.store.(oplog.AuthorLogstore); ok {
		ciphertext, err := al.FlatbufferCipher(book.pk)
		if err != nil {
			return err
		}

		file := qfs.NewMemfileBytes(book.fsLocation, ciphertext)
		book.fsLocation, err = book.fs.Put(ctx, file)
	}
	return err
}

// load reads the book dataset from book.fsLocation
func (book *Book) load(ctx context.Context) error {
	if al, ok := book.store.(oplog.AuthorLogstore); ok {
		f, err := book.fs.Get(ctx, book.fsLocation)
		if err != nil {
			if strings.Contains(err.Error(), "not found") {
				return ErrNotFound
			}
			return err
		}

		ciphertext, err := ioutil.ReadAll(f)
		if err != nil {
			return err
		}

		if err = al.UnmarshalFlatbufferCipher(ctx, book.pk, ciphertext); err != nil {
			return err
		}

		book.authorID = al.ID()
	}
	return nil
}

// WriteAuthorRename adds an operation updating the author's username
func (book *Book) WriteAuthorRename(ctx context.Context, newName string) error {
	if book == nil {
		return ErrNoLogbook
	}
	if !dsref.IsValidName(newName) {
		return fmt.Errorf("logbook: author name %q invalid", newName)
	}

	authorLog, err := book.authorLog(ctx)
	if err != nil {
		return err
	}
	authorLog.Append(oplog.Op{
		Type:      oplog.OpTypeAmend,
		Model:     AuthorModel,
		AuthorID:  book.AuthorID(),
		Name:      newName,
		Timestamp: NewTimestamp(),
	})

	if err := book.save(ctx); err != nil {
		return err
	}

	book.authorName = newName
	return nil
}

// WriteDatasetInit initializes a new dataset name within the author's namespace
// TODO (b5) - this should accept a username param. In the future "org" type
// users will want to delegate dataset creation to different keys, where the org
// username is used
func (book *Book) WriteDatasetInit(ctx context.Context, dsName string) (string, error) {
	if book == nil {
		return "", ErrNoLogbook
	}
	if dsName == "" {
		return "", fmt.Errorf("logbook: name is required to initialize a dataset")
	}
	if !dsref.IsValidName(dsName) {
		return "", fmt.Errorf("logbook: dataset name %q invalid", dsName)
	}
	if dsLog, err := book.DatasetRef(ctx, dsref.Ref{Username: book.Username(), Name: dsName}); err == nil {
		// check for "blank" logs, and remove them
		if len(dsLog.Ops) == 1 && len(dsLog.Logs) == 1 && len(dsLog.Logs[0].Ops) == 1 {
			log.Debugw("removing stranded reference", "ref", dsref.Ref{Username: book.Username(), Name: dsName})
			if err := book.RemoveLog(ctx, dsref.Ref{Username: book.Username(), Name: dsName}); err != nil {
				return "", fmt.Errorf("logbook: removing stray log: %w", err)
			}
		} else {
			return "", fmt.Errorf("logbook: dataset named %q already exists", dsName)
		}
	}

	authorLog, err := book.authorLog(ctx)
	if err != nil {
		return "", err
	}
	profileID := authorLog.ProfileID()

	log.Debugf("initializing name: '%s'", dsName)
	dsLog := oplog.InitLog(oplog.Op{
		Type:      oplog.OpTypeInit,
		Model:     DatasetModel,
		AuthorID:  book.AuthorID(),
		Name:      dsName,
		Timestamp: NewTimestamp(),
	})

	branch := oplog.InitLog(oplog.Op{
		Type:      oplog.OpTypeInit,
		Model:     BranchModel,
		AuthorID:  book.AuthorID(),
		Name:      DefaultBranchName,
		Timestamp: NewTimestamp(),
	})

	dsLog.AddChild(branch)

	authorLog.AddChild(dsLog)

	initID := dsLog.ID()

	// TODO(dlong): Perhaps in the future, pass the authorID (hash of the author creation
	// block) to the dscache, use that instead-of or in-addition-to the profileID.
	err = book.publisher.Publish(ctx, event.ETDatasetNameInit, event.DsChange{
		InitID:     initID,
		Username:   book.Username(),
		ProfileID:  profileID,
		PrettyName: dsName,
	})
	if err != nil {
		log.Error(err)
	}

	return initID, book.save(ctx)
}

// WriteDatasetRename marks renaming a dataset
func (book *Book) WriteDatasetRename(ctx context.Context, initID string, newName string) error {
	if book == nil {
		return ErrNoLogbook
	}
	if !dsref.IsValidName(newName) {
		return fmt.Errorf("logbook: new dataset name %q invalid", newName)
	}

	log.Debugf("WriteDatasetRename: '%s' -> '%s'", initID, newName)

	dsLog, err := book.datasetLog(ctx, initID)
	if err != nil {
		return err
	}

	if err := book.hasWriteAccess(dsLog.l); err != nil {
		return err
	}

	dsLog.Append(oplog.Op{
		Type:      oplog.OpTypeAmend,
		Model:     DatasetModel,
		Name:      newName,
		Timestamp: NewTimestamp(),
	})

	err = book.publisher.Publish(ctx, event.ETDatasetRename, event.DsChange{
		InitID:     initID,
		PrettyName: newName,
	})
	if err != nil {
		log.Error(err)
	}

	return book.save(ctx)
}

// RefToInitID converts a dsref to an initID by iterating the entire logbook looking for a match.
// This function is inefficient, iterating the entire set of operations in a log. Replacing this
// function call with mechanisms in dscache will fix this problem.
// TODO(dustmop): Don't depend on this function permanently, use a higher level resolver and
// convert all callers of this function to use that resolver's initID instead of converting a
// dsref yet again.
func (book *Book) RefToInitID(ref dsref.Ref) (string, error) {
	if book == nil {
		return "", ErrNoLogbook
	}

	// NOTE: Bad to retrieve the background context here, but HeadRef just ignores it anyway.
	ctx := context.Background()

	// HeadRef is inefficient, iterates the top two levels of the logbook.
	// Runs in O(M*N) where M = number of users, N = number of datasets per user.
	dsLog, err := book.store.HeadRef(ctx, ref.Username, ref.Name)
	if err != nil {
		if err == oplog.ErrNotFound {
			return "", ErrNotFound
		}
		return "", err
	}
	return dsLog.ID(), nil
}

// Return a strongly typed UserLog for the given profileID. Top level of the logbook.
func (book Book) userLog(ctx context.Context, profileID string) (*UserLog, error) {
	return nil, fmt.Errorf("TODO(dustmop): Not Implemented")
}

// Return a strongly typed UserLog for the author of the logbook.
func (book Book) authorLog(ctx context.Context) (*UserLog, error) {
	lg, err := book.store.Get(ctx, book.authorID)
	if err != nil {
		return nil, err
	}
	return newUserLog(lg), nil
}

// Return a strongly typed DatasetLog. Uses DatasetModel model.
func (book *Book) datasetLog(ctx context.Context, initID string) (*DatasetLog, error) {
	lg, err := book.store.Get(ctx, initID)
	if err != nil {
		return nil, err
	}
	return newDatasetLog(lg), nil
}

// Return a strongly typed BranchLog
func (book *Book) branchLog(ctx context.Context, initID string) (*BranchLog, error) {
	lg, err := book.store.Get(ctx, initID)
	if err != nil {
		return nil, err
	}
	if len(lg.Logs) != 1 {
		return nil, fmt.Errorf("expected dataset to have 1 branch, has %d", len(lg.Logs))
	}
	return newBranchLog(lg.Logs[0]), nil
}

// hasWriteAccess is a simple author-matching check
func (book *Book) hasWriteAccess(log *oplog.Log) error {
	if log.Ops[0].AuthorID != book.authorID {
		return fmt.Errorf("%w: you do not have write access", ErrAccessDenied)
	}
	return nil
}

// WriteDatasetDelete closes a dataset, marking it as deleted
func (book *Book) WriteDatasetDelete(ctx context.Context, initID string) error {
	if book == nil {
		return ErrNoLogbook
	}
	log.Debugf("WriteDatasetDelete: '%s'", initID)

	dsLog, err := book.datasetLog(ctx, initID)
	if err != nil {
		return err
	}

	if err := book.hasWriteAccess(dsLog.l); err != nil {
		return err
	}

	dsLog.Append(oplog.Op{
		Type:      oplog.OpTypeRemove,
		Model:     DatasetModel,
		Timestamp: NewTimestamp(),
	})

	err = book.publisher.Publish(ctx, event.ETDatasetDeleteAll, event.DsChange{
		InitID: initID,
	})
	if err != nil {
		log.Error(err)
	}

	return book.save(ctx)
}

// WriteVersionSave adds 1 or 2 operations marking the creation of a dataset
// version. If the run.State arg is nil only one commit operation is written
//
// If a run.State argument is non-nil two operations are written to the log,
// one op for the run followed by a commit op for the dataset save.
// If run.State is non-nil the dataset.Commit.RunID and rs.ID fields must match
func (book *Book) WriteVersionSave(ctx context.Context, initID string, ds *dataset.Dataset, rs *run.State) error {
	if book == nil {
		return ErrNoLogbook
	}

	log.Debugw("WriteVersionSave", "initID", initID)
	branchLog, err := book.branchLog(ctx, initID)
	if err != nil {
		return err
	}

	if err := book.hasWriteAccess(branchLog.l); err != nil {
		return err
	}

	if rs != nil {
		if rs.ID != ds.Commit.RunID {
			return fmt.Errorf("dataset.Commit.RunID does not match the provided run.ID")
		}
		book.appendTransformRun(branchLog, rs)
	}

	topIndex := book.appendVersionSave(branchLog, ds)
	// TODO(dlong): Think about how to handle a failure exactly here, what needs to be rolled back?
	err = book.save(ctx)
	if err != nil {
		return err
	}

	info := dsref.ConvertDatasetToVersionInfo(ds)

	err = book.publisher.Publish(ctx, event.ETDatasetCommitChange, event.DsChange{
		InitID:   initID,
		TopIndex: topIndex,
		HeadRef:  info.Path,
		Info:     &info,
	})
	if err != nil {
		log.Error(err)
	}

	return nil
}

// WriteTransformRun adds an operation to a log marking the execution of a
// dataset transform script
func (book *Book) WriteTransformRun(ctx context.Context, initID string, rs *run.State) error {
	if book == nil {
		return ErrNoLogbook
	}

	log.Debugf("WriteTransformRun: %s", initID)
	branchLog, err := book.branchLog(ctx, initID)
	if err != nil {
		return err
	}

	if err := book.hasWriteAccess(branchLog.l); err != nil {
		return err
	}

	book.appendTransformRun(branchLog, rs)
	// TODO(dlong): Think about how to handle a failure exactly here, what needs to be rolled back?
	err = book.save(ctx)
	if err != nil {
		return err
	}

	return nil
}

func (book *Book) appendVersionSave(blog *BranchLog, ds *dataset.Dataset) int {
	op := oplog.Op{
		Type:  oplog.OpTypeInit,
		Model: CommitModel,
		Ref:   ds.Path,
		Prev:  ds.PreviousPath,

		Timestamp: ds.Commit.Timestamp.UnixNano(),
		Note:      ds.Commit.Title,
	}

	if ds.Structure != nil {
		op.Size = int64(ds.Structure.Length)
	}
	if ds.Commit.RunID != "" {
		op.Relations = []string{fmt.Sprintf("%s%s", runIDRelPrefix, ds.Commit.RunID)}
	}

	blog.Append(op)

	return blog.Size() - 1
}

// appendTransformRun maps fields from run.State to an operation.
func (book *Book) appendTransformRun(blog *BranchLog, rs *run.State) int {
	op := oplog.Op{
		Type:  oplog.OpTypeInit,
		Model: RunModel,
		Ref:   rs.ID,
		Name:  fmt.Sprintf("%d", rs.Number),

		Size: int64(rs.Duration),
		Note: string(rs.Status),
	}

	if rs.StartTime != nil {
		op.Timestamp = rs.StartTime.UnixNano()
	}

	blog.Append(op)

	return blog.Size() - 1
}

// WriteVersionAmend adds an operation to a log when a dataset amends a commit
// TODO(dustmop): Currently unused by codebase, only called in tests.
func (book *Book) WriteVersionAmend(ctx context.Context, initID string, ds *dataset.Dataset) error {
	if book == nil {
		return ErrNoLogbook
	}
	log.Debugf("WriteVersionAmend: '%s'", initID)

	branchLog, err := book.branchLog(ctx, initID)
	if err != nil {
		return err
	}
	if err := book.hasWriteAccess(branchLog.l); err != nil {
		return err
	}

	branchLog.Append(oplog.Op{
		Type:  oplog.OpTypeAmend,
		Model: CommitModel,
		Ref:   ds.Path,
		Prev:  ds.PreviousPath,

		Timestamp: ds.Commit.Timestamp.UnixNano(),
		Note:      ds.Commit.Title,
	})

	return book.save(ctx)
}

// WriteVersionDelete adds an operation to a log marking a number of sequential
// versions from HEAD as deleted. Because logs are append-only, deletes are
// recorded as "tombstone" operations that mark removal.
func (book *Book) WriteVersionDelete(ctx context.Context, initID string, revisions int) error {
	if book == nil {
		return ErrNoLogbook
	}
	log.Debugf("WriteVersionDelete: %s, revisions: %d", initID, revisions)

	branchLog, err := book.branchLog(ctx, initID)
	if err != nil {
		return err
	}
	if err := book.hasWriteAccess(branchLog.l); err != nil {
		return err
	}

	branchLog.Append(oplog.Op{
		Type:  oplog.OpTypeRemove,
		Model: CommitModel,
		Size:  int64(revisions),
		// TODO (b5) - finish
	})

	// Calculate the commits after collapsing deletions found at the tail of history (most recent).
	items := branchToVersionInfos(branchLog, dsref.Ref{}, 0, -1, false)

	if len(items) > 0 {
		lastItem := items[len(items)-1]
		err = book.publisher.Publish(ctx, event.ETDatasetCommitChange, event.DsChange{
			InitID:   initID,
			TopIndex: len(items),
			HeadRef:  lastItem.Path,
			Info:     &lastItem,
		})
		if err != nil {
			log.Error(err)
		}
	}

	return book.save(ctx)
}

// WriteRemotePush adds an operation to a log marking the publication of a
// number of versions to a remote address. It returns a rollback function that
// removes the operation when called
func (book *Book) WriteRemotePush(ctx context.Context, initID string, revisions int, remoteAddr string) (l *oplog.Log, rollback func(ctx context.Context) error, err error) {
	if book == nil {
		return nil, nil, ErrNoLogbook
	}
	log.Debugf("WriteRemotePush: %s, revisions: %d, remote: %q", initID, revisions, remoteAddr)

	branchLog, err := book.branchLog(ctx, initID)
	if err != nil {
		return nil, nil, err
	}
	if err := book.hasWriteAccess(branchLog.l); err != nil {
		return nil, nil, err
	}

	branchLog.Append(oplog.Op{
		Type:      oplog.OpTypeInit,
		Model:     PushModel,
		Timestamp: NewTimestamp(),
		Size:      int64(revisions),
		Relations: []string{remoteAddr},
	})

	if err = book.save(ctx); err != nil {
		return nil, nil, err
	}

	var (
		rollbackOnce  sync.Once
		rollbackError error
	)
	// after successful save calling rollback drops the written push operation
	rollback = func(ctx context.Context) error {
		rollbackOnce.Do(func() {
			branchLog, err := book.branchLog(ctx, initID)
			if err != nil {
				rollbackError = err
				return
			}

			// TODO (b5) - the fact that this works means accessors are passing data that
			// if modified will be persisted on save, which may be a *major* source of
			// bugs if not handled correctly by packages that read & save logbook data
			// we should consider returning copies, and adding explicit methods for
			// modification.
			branchLog.l.Ops = branchLog.l.Ops[:len(branchLog.l.Ops)-1]
			rollbackError = book.save(ctx)
		})
		return rollbackError
	}

	sparseLog, err := book.UserDatasetBranchesLog(ctx, initID)
	if err != nil {
		rollback(ctx)
		return nil, rollback, err
	}

	return sparseLog, rollback, nil
}

// WriteRemoteDelete adds an operation to a log marking an unpublish request for
// a count of sequential versions from HEAD
func (book *Book) WriteRemoteDelete(ctx context.Context, initID string, revisions int, remoteAddr string) (l *oplog.Log, rollback func(ctx context.Context) error, err error) {
	if book == nil {
		return nil, nil, ErrNoLogbook
	}
	log.Debugf("WriteRemoteDelete: %s, revisions: %d, remote: %q", initID, revisions, remoteAddr)

	branchLog, err := book.branchLog(ctx, initID)
	if err != nil {
		return nil, nil, err
	}
	if err := book.hasWriteAccess(branchLog.l); err != nil {
		return nil, nil, err
	}

	branchLog.Append(oplog.Op{
		Type:      oplog.OpTypeRemove,
		Model:     PushModel,
		Timestamp: NewTimestamp(),
		Size:      int64(revisions),
		Relations: []string{remoteAddr},
	})

	if err = book.save(ctx); err != nil {
		return nil, nil, err
	}

	var (
		rollbackOnce  sync.Once
		rollbackError error
	)
	// after successful save calling rollback drops the written push operation
	rollback = func(ctx context.Context) error {
		rollbackOnce.Do(func() {
			branchLog, err := book.branchLog(ctx, initID)
			if err != nil {
				rollbackError = err
				return
			}
			branchLog.l.Ops = branchLog.l.Ops[:len(branchLog.l.Ops)-1]
			rollbackError = book.save(ctx)
		})
		return rollbackError
	}

	sparseLog, err := book.UserDatasetBranchesLog(ctx, initID)
	if err != nil {
		rollback(ctx)
		return nil, rollback, err
	}

	return sparseLog, rollback, nil
}

// ListAllLogs lists all of the logs in the logbook
func (book Book) ListAllLogs(ctx context.Context) ([]*oplog.Log, error) {
	return book.store.Logs(ctx, 0, -1)
}

// AllReferencedDatasetPaths scans an entire logbook looking for dataset paths
func (book *Book) AllReferencedDatasetPaths(ctx context.Context) (map[string]struct{}, error) {
	paths := map[string]struct{}{}
	logs, err := book.ListAllLogs(ctx)
	if err != nil {
		return nil, err
	}

	for _, l := range logs {
		addReferencedPaths(l, paths)
	}
	return paths, nil
}

func addReferencedPaths(log *oplog.Log, paths map[string]struct{}) {
	ps := []string{}
	for _, op := range log.Ops {
		if op.Model == CommitModel {
			switch op.Type {
			case oplog.OpTypeInit:
				ps = append(ps, op.Ref)
			case oplog.OpTypeRemove:
				ps = ps[:len(ps)-int(op.Size)]
			case oplog.OpTypeAmend:
				ps[len(ps)-1] = op.Ref
			}
		}
	}
	for _, p := range ps {
		paths[p] = struct{}{}
	}

	for _, l := range log.Logs {
		addReferencedPaths(l, paths)
	}
}

// Log gets a log for a given ID
func (book Book) Log(ctx context.Context, id string) (*oplog.Log, error) {
	return book.store.Get(ctx, id)
}

// ResolveRef finds the identifier & head path for a dataset reference
// implements resolve.NameResolver interface
func (book *Book) ResolveRef(ctx context.Context, ref *dsref.Ref) (string, error) {
	if book == nil {
		return "", dsref.ErrRefNotFound
	}

	initID, err := book.RefToInitID(*ref)
	if err != nil {
		return "", dsref.ErrRefNotFound
	}
	ref.InitID = initID

	var branchLog *BranchLog
	if ref.Path == "" {
		log.Debugw("finding branch log", "initID", initID)
		branchLog, err = book.branchLog(ctx, initID)
		if err != nil {
			return "", err
		}
		log.Debugw("found branch log", "initID", initID, "size", branchLog.Size(), "latestSavePath", book.latestSavePath(branchLog.l))
		ref.Path = book.latestSavePath(branchLog.l)
	}

	if ref.ProfileID == "" {
		if branchLog == nil {
			branchLog, err = book.branchLog(ctx, initID)
			if err != nil {
				return "", err
			}
		}

		authorLog, err := book.store.Get(ctx, branchLog.l.Author())
		if err != nil {
			return "", err
		}
		ref.ProfileID = authorLog.Ops[0].AuthorID
	}

	return "", nil
}

func (book *Book) latestSavePath(branchLog *oplog.Log) string {
	removes := 0

	for i := len(branchLog.Ops) - 1; i >= 0; i-- {
		op := branchLog.Ops[i]
		if op.Model == CommitModel {
			switch op.Type {
			case oplog.OpTypeRemove:
				removes += int(op.Size)
			case oplog.OpTypeInit, oplog.OpTypeAmend:
				if removes > 0 {
					removes--
				}
				if removes == 0 {
					return op.Ref
				}
			}
		}
	}
	return ""
}

// UserDatasetBranchesLog gets a user's log and a dataset reference.
// the returned log will be a user log with only one dataset log containing all
// known branches:
//   user
//     dataset
//       branch
//       branch
//       ...
func (book Book) UserDatasetBranchesLog(ctx context.Context, datasetInitID string) (*oplog.Log, error) {
	log.Debugf("UserDatasetBranchesLog datasetInitID=%q", datasetInitID)
	if datasetInitID == "" {
		return nil, fmt.Errorf("%w: cannot use the empty string as an init id", ErrNotFound)
	}

	dsLog, err := oplog.GetWithSparseAncestorsAllDescendants(ctx, book.store, datasetInitID)
	if err != nil {
		log.Debugf("store error=%q datasetInitID=%q", err, datasetInitID)
		return nil, err
	}

	return dsLog.Parent(), nil
}

// DatasetRef gets a dataset log and all branches. Dataset logs describe
// activity affecting an entire dataset. Things like dataset name changes and
// access control changes are kept in the dataset log
//
// currently all logs are hardcoded to only accept one branch name. This
// function will always return a single branch
//
// TODO(dustmop): Do not add new callers to this, transition away (preferring datasetLog instead),
// and delete it.
func (book Book) DatasetRef(ctx context.Context, ref dsref.Ref) (*oplog.Log, error) {
	if ref.Username == "" {
		return nil, fmt.Errorf("logbook: ref.Username is required")
	}
	if ref.Name == "" {
		return nil, fmt.Errorf("logbook: ref.Name is required")
	}

	return book.store.HeadRef(ctx, ref.Username, ref.Name)
}

// BranchRef gets a branch log for a dataset reference. Branch logs describe
// a line of commits
//
// currently all logs are hardcoded to only accept one branch name. This
// function always returns
//
// TODO(dustmop): Do not add new callers to this, transition away (preferring branchLog instead),
// and delete it.
func (book Book) BranchRef(ctx context.Context, ref dsref.Ref) (*oplog.Log, error) {
	if ref.Username == "" {
		return nil, fmt.Errorf("logbook: ref.Username is required")
	}
	if ref.Name == "" {
		return nil, fmt.Errorf("logbook: ref.Name is required")
	}

	return book.store.HeadRef(ctx, ref.Username, ref.Name, DefaultBranchName)
}

// SignLog populates the signature field of a log using the author's private key
func (book Book) SignLog(log *oplog.Log) error {
	return log.Sign(book.pk)
}

// LogBytes signs a log with this book's private key and writes to a flatbuffer
func (book Book) LogBytes(log *oplog.Log) ([]byte, error) {
	if err := book.SignLog(log); err != nil {
		return nil, err
	}
	return log.FlatbufferBytes(), nil
}

// DsrefAliasForLog parses log data into a dataset alias reference, populating
// only the username, name, and profileID the dataset.
// the passed in oplog must refer unambiguously to a dataset or branch.
// book.Log() returns exact log references
func DsrefAliasForLog(log *oplog.Log) (dsref.Ref, error) {
	ref := dsref.Ref{}
	if log == nil {
		return ref, fmt.Errorf("logbook: log is required")
	}
	if log.Model() != AuthorModel {
		return ref, fmt.Errorf("logbook: log isn't rooted as an author")
	}
	if len(log.Logs) != 1 {
		return ref, fmt.Errorf("logbook: ambiguous dataset reference")
	}

	ref = dsref.Ref{
		Username:  log.Name(),
		Name:      log.Logs[0].Name(),
		ProfileID: log.FirstOpAuthorID(),
	}

	return ref, nil
}

// MergeLog adds a log to the logbook, merging with any existing log data
func (book *Book) MergeLog(ctx context.Context, sender profile.Author, lg *oplog.Log) error {
	if book == nil {
		return ErrNoLogbook
	}
	// eventually access control will dictate which logs can be written by whom.
	// For now we only allow users to merge logs they've written
	// book will need access to a store of public keys before we can verify
	// signatures non-same-senders
	if err := lg.Verify(sender.AuthorPubKey()); err != nil {
		return err
	}

	if err := book.store.MergeLog(ctx, lg); err != nil {
		return err
	}

	return book.save(ctx)
}

// RemoveLog removes an entire log from a logbook
func (book *Book) RemoveLog(ctx context.Context, ref dsref.Ref) error {
	if book == nil {
		return ErrNoLogbook
	}
	book.store.RemoveLog(ctx, dsRefToLogPath(ref)...)
	return book.save(ctx)
}

func dsRefToLogPath(ref dsref.Ref) (path []string) {
	for _, str := range []string{
		ref.Username,
		ref.Name,
	} {
		path = append(path, str)
	}
	return path
}

// ConstructDatasetLog creates a sparse log from a connected dataset history
// where no prior log exists
// the given history MUST be ordered from oldest to newest commits
// TODO (b5) - this presently only works for datasets in an author's user
// namespace
func (book *Book) ConstructDatasetLog(ctx context.Context, ref dsref.Ref, history []*dataset.Dataset) error {
	if book == nil {
		return ErrNoLogbook
	}

	if _, err := book.RefToInitID(ref); err == nil {
		// if the log already exists, it will either as-or-more rich than this log,
		// refuse to overwrite
		return ErrLogTooShort
	}

	initID, err := book.WriteDatasetInit(ctx, ref.Name)
	if err != nil {
		return err
	}
	branchLog, err := book.branchLog(ctx, initID)
	if err != nil {
		return err
	}
	for _, ds := range history {
		book.appendVersionSave(branchLog, ds)
	}
	return book.save(ctx)
}

func commitOpRunID(op oplog.Op) string {
	for _, str := range op.Relations {
		if strings.HasPrefix(str, runIDRelPrefix) {
			return strings.TrimPrefix(str, runIDRelPrefix)
		}
	}
	return ""
}

func versionInfoFromOp(ref dsref.Ref, op oplog.Op) dsref.VersionInfo {
	return dsref.VersionInfo{
		Username:    ref.Username,
		ProfileID:   ref.ProfileID,
		Name:        ref.Name,
		Path:        op.Ref,
		CommitTime:  time.Unix(0, op.Timestamp),
		BodySize:    int(op.Size),
		CommitTitle: op.Note,
	}
}

func runItemFromOp(ref dsref.Ref, op oplog.Op) dsref.VersionInfo {
	return dsref.VersionInfo{
		Username:    ref.Username,
		ProfileID:   ref.ProfileID,
		Name:        ref.Name,
		CommitTime:  time.Unix(0, op.Timestamp),
		RunID:       op.Ref,
		RunStatus:   op.Note,
		RunDuration: int64(op.Size),
		// TODO(B5): When using qrimatic, I'd like to store the run number as a
		// name string here, but we currently don't have a way to plumb a run number
		// down from the qrimatic scheduler
		// RunNumber: strconv.ParseInt(op.Name),
	}
}

func addCommitDetailsToRunItem(li dsref.VersionInfo, op oplog.Op) dsref.VersionInfo {
	li.CommitTime = time.Unix(0, op.Timestamp)
	li.CommitTitle = op.Note
	li.BodySize = int(op.Size)
	li.Path = op.Ref
	return li
}

// Items collapses the history of a dataset branch into linear log items
func (book Book) Items(ctx context.Context, ref dsref.Ref, offset, limit int) ([]dsref.VersionInfo, error) {
	initID, err := book.RefToInitID(dsref.Ref{Username: ref.Username, Name: ref.Name})
	if err != nil {
		return nil, err
	}
	branchLog, err := book.branchLog(ctx, initID)
	if err != nil {
		return nil, err
	}

	return branchToVersionInfos(branchLog, ref, offset, limit, true), nil
}

// ConvertLogsToVersionInfos collapses the history of a dataset branch into linear log items
func ConvertLogsToVersionInfos(l *oplog.Log, ref dsref.Ref) []dsref.VersionInfo {
	return branchToVersionInfos(newBranchLog(l), ref, 0, -1, true)
}

// Items collapses the history of a dataset branch into linear log items
// If collapseAllDeletes is true, all delete operations will remove the refs before them. Otherwise,
// only refs at the end of history will be removed in this manner.
func branchToVersionInfos(blog *BranchLog, ref dsref.Ref, offset, limit int, collapseAllDeletes bool) []dsref.VersionInfo {
	refs := []dsref.VersionInfo{}
	deleteAtEnd := 0
	for _, op := range blog.Ops() {
		switch op.Model {
		case CommitModel:
			switch op.Type {
			case oplog.OpTypeInit:
				// run operations & commit operations often occur next to each other in
				// the log.
				// if the last item in the slice has a runID that matches a runID resource
				// from this commit, combine them into one Log item that describes both
				// the run and the save
				commitRunID := commitOpRunID(op)
				if commitRunID != "" && len(refs) > 0 && commitRunID == refs[len(refs)-1].RunID {
					refs[len(refs)-1] = addCommitDetailsToRunItem(refs[len(refs)-1], op)
				} else {
					refs = append(refs, versionInfoFromOp(ref, op))
				}
			case oplog.OpTypeAmend:
				deleteAtEnd = 0
				refs[len(refs)-1] = versionInfoFromOp(ref, op)
			case oplog.OpTypeRemove:
				if collapseAllDeletes {
					refs = refs[:len(refs)-int(op.Size)]
				} else {
					deleteAtEnd += int(op.Size)
				}
			}
		case RunModel:
			// runs are only ever "init" op type
			refs = append(refs, runItemFromOp(ref, op))
		case PushModel:
			switch op.Type {
			case oplog.OpTypeInit:
				for i := 1; i <= int(op.Size); i++ {
					refs[len(refs)-i].Published = true
				}
			case oplog.OpTypeRemove:
				for i := 1; i <= int(op.Size); i++ {
					refs[len(refs)-i].Published = false
				}
			}
		}
	}

	if deleteAtEnd > 0 {
		if deleteAtEnd < len(refs) {
			refs = refs[:len(refs)-deleteAtEnd]
		} else {
			refs = []dsref.VersionInfo{}
		}
	}

	// reverse the slice, placing newest first
	// https://github.com/golang/go/wiki/SliceTricks#reversing
	for i := len(refs)/2 - 1; i >= 0; i-- {
		opp := len(refs) - 1 - i
		refs[i], refs[opp] = refs[opp], refs[i]
	}

	if offset > len(refs) {
		offset = len(refs)
	}
	refs = refs[offset:]

	if limit < len(refs) && limit != -1 {
		refs = refs[:limit]
	}

	return refs
}

// LogEntry is a simplified representation of a log operation
type LogEntry struct {
	Timestamp time.Time
	Author    string
	Action    string
	Note      string
}

// String formats a LogEntry as a String
func (l LogEntry) String() string {
	return fmt.Sprintf("%s\t%s\t%s\t%s", l.Timestamp.Format(time.Kitchen), l.Author, l.Action, l.Note)
}

// LogEntries returns a summarized "line-by-line" representation of a log for a
// given dataset reference
func (book Book) LogEntries(ctx context.Context, ref dsref.Ref, offset, limit int) ([]LogEntry, error) {
	l, err := book.BranchRef(ctx, ref)
	if err != nil {
		return nil, err
	}

	res := []LogEntry{}
	for _, op := range l.Ops {
		if offset > 0 {
			offset--
			continue
		}
		res = append(res, logEntryFromOp(ref.Username, op))
		if len(res) == limit {
			break
		}
	}

	return res, nil
}

var actionStrings = map[uint32][3]string{
	AuthorModel:  {"create profile", "update profile", "delete profile"},
	DatasetModel: {"init dataset", "rename dataset", "delete dataset"},
	BranchModel:  {"init branch", "rename branch", "delete branch"},
	CommitModel:  {"save commit", "amend commit", "remove commit"},
	PushModel:    {"publish", "", "unpublish"},
	ACLModel:     {"update access", "update access", "remove all access"},
}

func logEntryFromOp(author string, op oplog.Op) LogEntry {
	note := op.Note
	if note == "" && op.Name != "" {
		note = op.Name
	}
	return LogEntry{
		Timestamp: time.Unix(0, op.Timestamp),
		Author:    author,
		Action:    actionStrings[op.Model][int(op.Type)-1],
		Note:      note,
	}
}

// PlainLogs returns plain-old-data representations of the logs, intended for serialization
func (book Book) PlainLogs(ctx context.Context) ([]PlainLog, error) {
	raw, err := book.store.Logs(ctx, 0, -1)
	if err != nil {
		return nil, err
	}

	logs := make([]PlainLog, len(raw))
	for i, l := range raw {
		logs[i] = NewPlainLog(l)
	}
	return logs, nil
}

// SummaryString prints the entire hierarchy of logbook model/ID/opcount/name in
// a single string
func (book Book) SummaryString(ctx context.Context) string {
	logs, err := book.store.Logs(ctx, 0, -1)
	if err != nil {
		return fmt.Sprintf("error getting diagnostics: %q", err)
	}

	builder := &strings.Builder{}
	for _, user := range logs {
		builder.WriteString(fmt.Sprintf("%s %s %d %s\n", ModelString(user.Model()), user.ID(), len(user.Ops), user.Name()))
		for _, dataset := range user.Logs {
			builder.WriteString(fmt.Sprintf("  %s %s %d %s\n", ModelString(dataset.Model()), dataset.ID(), len(dataset.Ops), dataset.Name()))
			for _, branch := range dataset.Logs {
				builder.WriteString(fmt.Sprintf("    %s %s %d %s\n", ModelString(branch.Model()), branch.ID(), len(branch.Ops), branch.Name()))
			}
		}
	}

	return builder.String()
}

// PlainLog is a human-oriented representation of oplog.Log intended for serialization
type PlainLog struct {
	Ops  []PlainOp  `json:"ops,omitempty"`
	Logs []PlainLog `json:"logs,omitempty"`
}

// NewPlainLog converts an oplog to a plain log
func NewPlainLog(lg *oplog.Log) PlainLog {
	if lg == nil {
		return PlainLog{}
	}

	ops := make([]PlainOp, len(lg.Ops))
	for i, o := range lg.Ops {
		ops[i] = newPlainOp(o)
	}

	var ls []PlainLog
	if len(lg.Logs) > 0 {
		ls = make([]PlainLog, len(lg.Logs))
		for i, l := range lg.Logs {
			ls[i] = NewPlainLog(l)
		}
	}

	return PlainLog{
		Ops:  ops,
		Logs: ls,
	}
}

// PlainOp is a human-oriented representation of oplog.Op intended for serialization
type PlainOp struct {
	// type of operation
	Type string `json:"type,omitempty"`
	// data model to operate on
	Model string `json:"model,omitempty"`
	// identifier of data this operation is documenting
	Ref string `json:"ref,omitempty"`
	// previous reference in a causal history
	Prev string `json:"prev,omitempty"`
	// references this operation relates to. usage is operation type-dependant
	Relations []string `json:"relations,omitempty"`
	// human-readable name for the reference
	Name string `json:"name,omitempty"`
	// identifier for author
	AuthorID string `json:"authorID,omitempty"`
	// operation timestamp, for annotation purposes only
	Timestamp time.Time `json:"timestamp,omitempty"`
	// size of the referenced value in bytes
	Size int64 `json:"size,omitempty"`
	// operation annotation for users. eg: commit title
	Note string `json:"note,omitempty"`
}

func newPlainOp(op oplog.Op) PlainOp {
	return PlainOp{
		Type:      opTypeString(op.Type),
		Model:     ModelString(op.Model),
		Ref:       op.Ref,
		Prev:      op.Prev,
		Relations: op.Relations,
		Name:      op.Name,
		AuthorID:  op.AuthorID,
		Timestamp: time.Unix(0, op.Timestamp),
		Size:      op.Size,
		Note:      op.Note,
	}
}

func opTypeString(op oplog.OpType) string {
	switch op {
	case oplog.OpTypeInit:
		return "init"
	case oplog.OpTypeAmend:
		return "amend"
	case oplog.OpTypeRemove:
		return "remove"
	default:
		return ""
	}
}

func refFromDataset(ds *dataset.Dataset) dsref.Ref {
	return dsref.Ref{
		Username:  ds.Peername,
		ProfileID: ds.ProfileID,
		Name:      ds.Name,
		Path:      ds.Path,
	}
}

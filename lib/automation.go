package lib

import (
	"context"
	"fmt"
	"io"

	"github.com/qri-io/dataset"
	"github.com/qri-io/dataset/preview"
	"github.com/qri-io/ioes"
	"github.com/qri-io/qri/dsref"
	"github.com/qri-io/qri/event"
	"github.com/qri-io/qri/scheduler"
	"github.com/qri-io/qri/transform"
	"github.com/qri-io/qri/transform/run"
	"github.com/qri-io/qri/workflow"
)

// AutomationMethods groups together methods for transforms
// TODO(b5): expand apply methods:
//   automation.apply             // Done!
//   automation.workflows         // list local workflows
//   automation.workflow          // get a workflow
//   automation.saveWorkflow      // "deploy" in qrimatic UI, create/update a workflow
//   automation.removeWorkflow    // "undeploy" in qrimatic UI
//   automation.runs              // list automation runs
//   automation.run               // get automation run log
type AutomationMethods struct {
	d dispatcher
}

// Name returns the name of this method group
func (m AutomationMethods) Name() string {
	return "automation"
}

// Attributes defines attributes for each method
func (m AutomationMethods) Attributes() map[string]AttributeSet {
	return map[string]AttributeSet{
		"apply": {AEApply, "POST"},
	}
}

// ApplyParams are parameters for the apply command
type ApplyParams struct {
	Refstr    string
	Transform *dataset.Transform
	Secrets   map[string]string
	Wait      bool

	Source string
	// TODO(arqu): substitute with websockets when working over the wire
	ScriptOutput io.Writer `json:"-"`
}

// Validate returns an error if ApplyParams fields are in an invalid state
func (p *ApplyParams) Validate() error {
	if p.Refstr == "" && p.Transform == nil {
		return fmt.Errorf("one or both of Reference, Transform are required")
	}
	return nil
}

// ApplyResult is the result of an apply command
type ApplyResult struct {
	Data  *dataset.Dataset
	RunID string `json:"runID"`
}

// Apply runs a transform script
func (m AutomationMethods) Apply(ctx context.Context, p *ApplyParams) (*ApplyResult, error) {
	got, _, err := m.d.Dispatch(ctx, dispatchMethodName(m, "apply"), p)
	if res, ok := got.(*ApplyResult); ok {
		return res, err
	}
	return nil, dispatchReturnError(got, err)
}

// Implementations for transform methods follow

// automationImpl holds the method implementations for transforms
type automationImpl struct{}

// Apply runs a transform script
func (automationImpl) Apply(scp scope, p *ApplyParams) (*ApplyResult, error) {
	ctx := scp.Context()

	var err error
	ref := dsref.Ref{}
	if p.Refstr != "" {
		ref, _, err = scp.ParseAndResolveRefWithWorkingDir(ctx, p.Refstr, "")
		if err != nil {
			return nil, err
		}
	}

	ds := &dataset.Dataset{}
	if !ref.IsEmpty() {
		ds.Name = ref.Name
		ds.Peername = ref.Username
	}
	if p.Transform != nil {
		ds.Transform = p.Transform
		ds.Transform.OpenScriptFile(ctx, scp.Filesystem())
	}

	// allocate an ID for the transform, for now just log the events it produces
	runID := run.NewID()
	scp.Bus().SubscribeID(func(ctx context.Context, e event.Event) error {
		go func() {
			log.Debugw("apply transform event", "type", e.Type, "payload", e.Payload)
			if e.Type == event.ETTransformPrint {
				if msg, ok := e.Payload.(event.TransformMessage); ok {
					if p.ScriptOutput != nil {
						io.WriteString(p.ScriptOutput, msg.Msg)
						io.WriteString(p.ScriptOutput, "\n")
					}
				}
			}
		}()
		return nil
	}, runID)

	scriptOut := p.ScriptOutput
	loader := scp.ParseResolveFunc()

	transformer := transform.NewTransformer(scp.AppContext(), loader, scp.Bus())
	if err = transformer.Apply(ctx, ds, runID, p.Wait, scriptOut, p.Secrets); err != nil {
		return nil, err
	}

	res := &ApplyResult{}
	if p.Wait {
		ds, err := preview.Create(ctx, ds)
		if err != nil {
			return nil, err
		}
		res.Data = ds
	}
	res.RunID = runID
	return res, nil
}

// newInstanceRunnerFactory returns a factory function that produces a workflow
// runner from a qri instance
func newInstanceRunnerFactory(inst *Instance) func(ctx context.Context) scheduler.RunWorkflowFunc {
	return func(ctx context.Context) scheduler.RunWorkflowFunc {
		return func(ctx context.Context, streams ioes.IOStreams, w *workflow.Workflow) error {
			runID := run.NewID()
			p := &SaveParams{
				Ref: w.DatasetID,
				Dataset: &dataset.Dataset{
					Commit: &dataset.Commit{
						RunID: runID,
					},
				},
				Apply: true,
			}
			_, err := inst.Dataset().Save(ctx, p)
			return err
		}
	}
}

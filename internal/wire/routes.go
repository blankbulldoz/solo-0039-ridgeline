package wire

import (
	"net/http"

	"ridgeline/internal/engine"
	"ridgeline/internal/model"
)

func Handler(e *engine.Engine) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ridge/v1/health", func(w http.ResponseWriter, r *http.Request) {
		respond(w, map[string]string{"service": "ridgeline", "state": "ready"}, nil)
	})
	mux.HandleFunc("POST /ridge/v1/spaces", func(w http.ResponseWriter, r *http.Request) {
		handleBody(w, r, func(n model.Namespace) (any, error) { return e.Create(n) })
	})
	base := "/ridge/v1/spaces/{space}"
	mux.HandleFunc("GET "+base, func(w http.ResponseWriter, r *http.Request) {
		out, err := e.Namespace(r.PathValue("space"))
		respond(w, out, err)
	})
	mux.HandleFunc("PUT "+base+"/mode", func(w http.ResponseWriter, r *http.Request) {
		handleBody(w, r, func(input struct {
			ReadOnly bool `json:"read_only"`
		}) (any, error) {
			return e.ReadOnly(r.PathValue("space"), input.ReadOnly)
		})
	})
	mux.HandleFunc("POST "+base+"/points", func(w http.ResponseWriter, r *http.Request) {
		handleBody(w, r, func(input model.WriteRequest) (any, error) {
			return e.Write(r.PathValue("space"), input)
		})
	})
	mux.HandleFunc("POST "+base+"/series", func(w http.ResponseWriter, r *http.Request) {
		handleBody(w, r, func(input struct {
			Matchers []model.Matcher `json:"matchers"`
		}) (any, error) {
			return e.Discover(r.PathValue("space"), input.Matchers)
		})
	})
	mux.HandleFunc("POST "+base+"/range", func(w http.ResponseWriter, r *http.Request) {
		handleBody(w, r, func(q model.Query) (any, error) {
			return e.Range(r.PathValue("space"), q)
		})
	})
	mux.HandleFunc("POST "+base+"/aggregate", func(w http.ResponseWriter, r *http.Request) {
		handleBody(w, r, func(q model.Query) (any, error) {
			return e.Aggregate(r.PathValue("space"), q)
		})
	})
	mux.HandleFunc("POST "+base+"/explain", func(w http.ResponseWriter, r *http.Request) {
		handleBody(w, r, func(q model.Query) (any, error) {
			return e.Explain(r.PathValue("space"), q)
		})
	})
	mux.HandleFunc("POST "+base+"/seal", func(w http.ResponseWriter, r *http.Request) {
		out, err := e.Seal(r.PathValue("space"))
		respond(w, out, err)
	})
	mux.HandleFunc("GET "+base+"/blocks", func(w http.ResponseWriter, r *http.Request) {
		out, err := e.Blocks(r.PathValue("space"))
		respond(w, out, err)
	})
	mux.HandleFunc("POST "+base+"/deletions", func(w http.ResponseWriter, r *http.Request) {
		handleBody(w, r, func(q model.Query) (any, error) {
			return e.Delete(r.PathValue("space"), q)
		})
	})
	mux.HandleFunc("GET "+base+"/deletions", func(w http.ResponseWriter, r *http.Request) {
		out, err := e.Deletions(r.PathValue("space"))
		respond(w, out, err)
	})
	mux.HandleFunc("POST "+base+"/retention", func(w http.ResponseWriter, r *http.Request) {
		handleBody(w, r, func(input struct {
			Now int64 `json:"now"`
		}) (any, error) {
			return e.Retain(r.PathValue("space"), input.Now)
		})
	})
	mux.HandleFunc("POST "+base+"/compact", func(w http.ResponseWriter, r *http.Request) {
		out, err := e.Compact(r.PathValue("space"))
		respond(w, out, err)
	})
	mux.HandleFunc("PUT "+base+"/rollups/{rollup}", func(w http.ResponseWriter, r *http.Request) {
		handleBody(w, r, func(q model.Query) (any, error) {
			return e.Materialize(r.PathValue("space"), r.PathValue("rollup"), q)
		})
	})
	mux.HandleFunc("GET "+base+"/rollups/{rollup}", func(w http.ResponseWriter, r *http.Request) {
		out, err := e.Rollup(r.PathValue("space"), r.PathValue("rollup"))
		respond(w, out, err)
	})
	mux.HandleFunc("GET "+base+"/rules", func(w http.ResponseWriter, r *http.Request) {
		out, err := e.Rules(r.PathValue("space"))
		respond(w, out, err)
	})
	mux.HandleFunc("PUT "+base+"/rules/{rule}", func(w http.ResponseWriter, r *http.Request) {
		handleBody(w, r, func(in model.RuleInput) (any, error) {
			return e.PutRule(r.PathValue("space"), r.PathValue("rule"), in)
		})
	})
	mux.HandleFunc("GET "+base+"/rules/{rule}", func(w http.ResponseWriter, r *http.Request) {
		out, err := e.Rule(r.PathValue("space"), r.PathValue("rule"))
		respond(w, out, err)
	})
	mux.HandleFunc("DELETE "+base+"/rules/{rule}", func(w http.ResponseWriter, r *http.Request) {
		out, err := e.DeleteRule(r.PathValue("space"), r.PathValue("rule"))
		respond(w, out, err)
	})
	mux.HandleFunc("GET "+base+"/rules/{rule}/revisions", func(w http.ResponseWriter, r *http.Request) {
		out, err := e.RuleRevisions(r.PathValue("space"), r.PathValue("rule"))
		respond(w, out, err)
	})
	mux.HandleFunc("POST "+base+"/rules/{rule}/runs", func(w http.ResponseWriter, r *http.Request) {
		handleBody(w, r, func(in model.RuleRunRequest) (any, error) {
			return e.RunRules(r.PathValue("space"), r.PathValue("rule"), in)
		})
	})
	mux.HandleFunc("GET "+base+"/rules/{rule}/runs", func(w http.ResponseWriter, r *http.Request) {
		out, err := e.RuleRunsFor(r.PathValue("space"), r.PathValue("rule"))
		respond(w, out, err)
	})
	mux.HandleFunc("GET "+base+"/runs", func(w http.ResponseWriter, r *http.Request) {
		out, err := e.RuleRunsFor(r.PathValue("space"), "")
		respond(w, out, err)
	})
	mux.HandleFunc("GET "+base+"/runs/{run}", func(w http.ResponseWriter, r *http.Request) {
		out, err := e.RuleRun(r.PathValue("space"), r.PathValue("run"))
		respond(w, out, err)
	})
	mux.HandleFunc("GET /ridge/v1/image", func(w http.ResponseWriter, r *http.Request) {
		serveImage(e, w, r)
	})
	return mux
}

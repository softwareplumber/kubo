package coreapi

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"

	coreiface "github.com/ipfs/boxo/coreiface"
	caopts "github.com/ipfs/boxo/coreiface/options"
	dag "github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/boxo/ipld/merkledag/dagutils"
	ft "github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/path"
	pin "github.com/ipfs/boxo/pinning/pinner"
	cid "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/ipfs/kubo/tracing"
)

const inputLimit = 2 << 20

type ObjectAPI CoreAPI

type Link struct {
	Name, Hash string
	Size       uint64
}

type Node struct {
	Links []Link
	Data  string
}

func (api *ObjectAPI) New(ctx context.Context, opts ...caopts.ObjectNewOption) (ipld.Node, error) {
	ctx, span := tracing.Span(ctx, "CoreAPI.ObjectAPI", "New")
	defer span.End()

	options, err := caopts.ObjectNewOptions(opts...)
	if err != nil {
		return nil, err
	}

	var n ipld.Node
	switch options.Type {
	case "empty":
		n = new(dag.ProtoNode)
	case "unixfs-dir":
		n = ft.EmptyDirNode()
	default:
		return nil, fmt.Errorf("unknown node type: %s", options.Type)
	}

	err = api.dag.Add(ctx, n)
	if err != nil {
		return nil, err
	}
	return n, nil
}

func (api *ObjectAPI) Put(ctx context.Context, src io.Reader, opts ...caopts.ObjectPutOption) (path.ImmutablePath, error) {
	ctx, span := tracing.Span(ctx, "CoreAPI.ObjectAPI", "Put")
	defer span.End()

	options, err := caopts.ObjectPutOptions(opts...)
	if err != nil {
		return path.ImmutablePath{}, err
	}
	span.SetAttributes(
		attribute.Bool("pin", options.Pin),
		attribute.String("datatype", options.DataType),
		attribute.String("inputenc", options.InputEnc),
	)

	data, err := io.ReadAll(io.LimitReader(src, inputLimit+10))
	if err != nil {
		return path.ImmutablePath{}, err
	}

	var dagnode *dag.ProtoNode
	switch options.InputEnc {
	case "json":
		node := new(Node)
		decoder := json.NewDecoder(bytes.NewReader(data))
		decoder.DisallowUnknownFields()
		err = decoder.Decode(node)
		if err != nil {
			return path.ImmutablePath{}, err
		}

		dagnode, err = deserializeNode(node, options.DataType)
		if err != nil {
			return path.ImmutablePath{}, err
		}

	case "protobuf":
		dagnode, err = dag.DecodeProtobuf(data)

	case "xml":
		node := new(Node)
		err = xml.Unmarshal(data, node)
		if err != nil {
			return path.ImmutablePath{}, err
		}

		dagnode, err = deserializeNode(node, options.DataType)
		if err != nil {
			return path.ImmutablePath{}, err
		}

	default:
		return path.ImmutablePath{}, errors.New("unknown object encoding")
	}

	if err != nil {
		return path.ImmutablePath{}, err
	}

	if options.Pin {
		defer api.blockstore.PinLock(ctx).Unlock(ctx)
	}

	err = api.dag.Add(ctx, dagnode)
	if err != nil {
		return path.ImmutablePath{}, err
	}

	if options.Pin {
		if err := api.pinning.PinWithMode(ctx, dagnode.Cid(), pin.Recursive); err != nil {
			return path.ImmutablePath{}, err
		}

		err = api.pinning.Flush(ctx)
		if err != nil {
			return path.ImmutablePath{}, err
		}
	}

	return path.FromCid(dagnode.Cid()), nil
}

func (api *ObjectAPI) Get(ctx context.Context, path path.Path) (ipld.Node, error) {
	ctx, span := tracing.Span(ctx, "CoreAPI.ObjectAPI", "Get", trace.WithAttributes(attribute.String("path", path.String())))
	defer span.End()
	return api.core().ResolveNode(ctx, path)
}

func (api *ObjectAPI) Data(ctx context.Context, path path.Path) (io.Reader, error) {
	ctx, span := tracing.Span(ctx, "CoreAPI.ObjectAPI", "Data", trace.WithAttributes(attribute.String("path", path.String())))
	defer span.End()

	nd, err := api.core().ResolveNode(ctx, path)
	if err != nil {
		return nil, err
	}

	pbnd, ok := nd.(*dag.ProtoNode)
	if !ok {
		return nil, dag.ErrNotProtobuf
	}

	return bytes.NewReader(pbnd.Data()), nil
}

func (api *ObjectAPI) Links(ctx context.Context, path path.Path) ([]*ipld.Link, error) {
	ctx, span := tracing.Span(ctx, "CoreAPI.ObjectAPI", "Links", trace.WithAttributes(attribute.String("path", path.String())))
	defer span.End()

	nd, err := api.core().ResolveNode(ctx, path)
	if err != nil {
		return nil, err
	}

	links := nd.Links()
	out := make([]*ipld.Link, len(links))
	for n, l := range links {
		out[n] = (*ipld.Link)(l)
	}

	return out, nil
}

func (api *ObjectAPI) Stat(ctx context.Context, path path.Path) (*coreiface.ObjectStat, error) {
	ctx, span := tracing.Span(ctx, "CoreAPI.ObjectAPI", "Stat", trace.WithAttributes(attribute.String("path", path.String())))
	defer span.End()

	nd, err := api.core().ResolveNode(ctx, path)
	if err != nil {
		return nil, err
	}

	stat, err := nd.Stat()
	if err != nil {
		return nil, err
	}

	out := &coreiface.ObjectStat{
		Cid:            nd.Cid(),
		NumLinks:       stat.NumLinks,
		BlockSize:      stat.BlockSize,
		LinksSize:      stat.LinksSize,
		DataSize:       stat.DataSize,
		CumulativeSize: stat.CumulativeSize,
	}

	return out, nil
}

func (api *ObjectAPI) AddLink(ctx context.Context, base path.Path, name string, child path.Path, opts ...caopts.ObjectAddLinkOption) (path.ImmutablePath, error) {
	ctx, span := tracing.Span(ctx, "CoreAPI.ObjectAPI", "AddLink", trace.WithAttributes(
		attribute.String("base", base.String()),
		attribute.String("name", name),
		attribute.String("child", child.String()),
	))
	defer span.End()

	options, err := caopts.ObjectAddLinkOptions(opts...)
	if err != nil {
		return path.ImmutablePath{}, err
	}
	span.SetAttributes(attribute.Bool("create", options.Create))

	baseNd, err := api.core().ResolveNode(ctx, base)
	if err != nil {
		return path.ImmutablePath{}, err
	}

	childNd, err := api.core().ResolveNode(ctx, child)
	if err != nil {
		return path.ImmutablePath{}, err
	}

	basePb, ok := baseNd.(*dag.ProtoNode)
	if !ok {
		return path.ImmutablePath{}, dag.ErrNotProtobuf
	}

	var createfunc func() *dag.ProtoNode
	if options.Create {
		createfunc = ft.EmptyDirNode
	}

	e := dagutils.NewDagEditor(basePb, api.dag)

	err = e.InsertNodeAtPath(ctx, name, childNd, createfunc)
	if err != nil {
		return path.ImmutablePath{}, err
	}

	nnode, err := e.Finalize(ctx, api.dag)
	if err != nil {
		return path.ImmutablePath{}, err
	}

	return path.FromCid(nnode.Cid()), nil
}

func (api *ObjectAPI) RmLink(ctx context.Context, base path.Path, link string) (path.ImmutablePath, error) {
	ctx, span := tracing.Span(ctx, "CoreAPI.ObjectAPI", "RmLink", trace.WithAttributes(
		attribute.String("base", base.String()),
		attribute.String("link", link)),
	)
	defer span.End()

	baseNd, err := api.core().ResolveNode(ctx, base)
	if err != nil {
		return path.ImmutablePath{}, err
	}

	basePb, ok := baseNd.(*dag.ProtoNode)
	if !ok {
		return path.ImmutablePath{}, dag.ErrNotProtobuf
	}

	e := dagutils.NewDagEditor(basePb, api.dag)

	err = e.RmLink(ctx, link)
	if err != nil {
		return path.ImmutablePath{}, err
	}

	nnode, err := e.Finalize(ctx, api.dag)
	if err != nil {
		return path.ImmutablePath{}, err
	}

	return path.FromCid(nnode.Cid()), nil
}

func (api *ObjectAPI) AppendData(ctx context.Context, path path.Path, r io.Reader) (path.ImmutablePath, error) {
	ctx, span := tracing.Span(ctx, "CoreAPI.ObjectAPI", "AppendData", trace.WithAttributes(attribute.String("path", path.String())))
	defer span.End()

	return api.patchData(ctx, path, r, true)
}

func (api *ObjectAPI) SetData(ctx context.Context, path path.Path, r io.Reader) (path.ImmutablePath, error) {
	ctx, span := tracing.Span(ctx, "CoreAPI.ObjectAPI", "SetData", trace.WithAttributes(attribute.String("path", path.String())))
	defer span.End()

	return api.patchData(ctx, path, r, false)
}

func (api *ObjectAPI) patchData(ctx context.Context, p path.Path, r io.Reader, appendData bool) (path.ImmutablePath, error) {
	nd, err := api.core().ResolveNode(ctx, p)
	if err != nil {
		return path.ImmutablePath{}, err
	}

	pbnd, ok := nd.(*dag.ProtoNode)
	if !ok {
		return path.ImmutablePath{}, dag.ErrNotProtobuf
	}

	data, err := io.ReadAll(r)
	if err != nil {
		return path.ImmutablePath{}, err
	}

	if appendData {
		data = append(pbnd.Data(), data...)
	}
	pbnd.SetData(data)

	err = api.dag.Add(ctx, pbnd)
	if err != nil {
		return path.ImmutablePath{}, err
	}

	return path.FromCid(pbnd.Cid()), nil
}

func (api *ObjectAPI) Diff(ctx context.Context, before path.Path, after path.Path) ([]coreiface.ObjectChange, error) {
	ctx, span := tracing.Span(ctx, "CoreAPI.ObjectAPI", "Diff", trace.WithAttributes(
		attribute.String("before", before.String()),
		attribute.String("after", after.String()),
	))
	defer span.End()

	beforeNd, err := api.core().ResolveNode(ctx, before)
	if err != nil {
		return nil, err
	}

	afterNd, err := api.core().ResolveNode(ctx, after)
	if err != nil {
		return nil, err
	}

	changes, err := dagutils.Diff(ctx, api.dag, beforeNd, afterNd)
	if err != nil {
		return nil, err
	}

	out := make([]coreiface.ObjectChange, len(changes))
	for i, change := range changes {
		out[i] = coreiface.ObjectChange{
			Type: coreiface.ChangeType(change.Type),
			Path: change.Path,
		}

		if change.Before.Defined() {
			out[i].Before = path.FromCid(change.Before)
		}

		if change.After.Defined() {
			out[i].After = path.FromCid(change.After)
		}
	}

	return out, nil
}

func (api *ObjectAPI) core() coreiface.CoreAPI {
	return (*CoreAPI)(api)
}

func deserializeNode(nd *Node, dataFieldEncoding string) (*dag.ProtoNode, error) {
	dagnode := new(dag.ProtoNode)
	switch dataFieldEncoding {
	case "text":
		dagnode.SetData([]byte(nd.Data))
	case "base64":
		data, err := base64.StdEncoding.DecodeString(nd.Data)
		if err != nil {
			return nil, err
		}
		dagnode.SetData(data)
	default:
		return nil, fmt.Errorf("unknown data field encoding")
	}

	links := make([]*ipld.Link, len(nd.Links))
	for i, link := range nd.Links {
		c, err := cid.Decode(link.Hash)
		if err != nil {
			return nil, err
		}
		links[i] = &ipld.Link{
			Name: link.Name,
			Size: link.Size,
			Cid:  c,
		}
	}
	if err := dagnode.SetLinks(links); err != nil {
		return nil, err
	}

	return dagnode, nil
}

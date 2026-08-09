package hyperliquid

import (
	"context"
	"fmt"
)

// CancelOpt customizes a cancel action. Follows the ExchangeOpt/InfoOpt idiom so
// existing call sites keep compiling.
type CancelOpt func(*cancelOpts)

type cancelOpts struct{ fast bool }

func applyCancelOpts(opts []CancelOpt) cancelOpts {
	var o cancelOpts
	for _, fn := range opts {
		if fn != nil {
			fn(&o)
		}
	}
	return o
}

// WithFastCancel sets HyperCore's `f` flag on the cancel action.
//
// The docs describe it as inert for now — "Currently fast has no other effect. In
// a future network upgrade, cancel actions will be prioritized in the mempool if
// and only if fast = true" — but that is already out of date: measured against
// mainnet (xyz:AMD, n=30, WS submit) cancel round-trip is p50 664ms without it and
// p50 335ms with it, with zero overlap across 60 samples.
//
// The one documented restriction: a fast cancel is REJECTED if it refers to a
// trigger order. Do not set this on TP/SL cancels.
func WithFastCancel() CancelOpt {
	return func(o *cancelOpts) { o.fast = true }
}

type (
	CancelOrderRequest struct {
		Coin    string
		OrderID int64
	}

	CancelOrderResponse struct {
		Statuses MixedArray
	}
)

func (e *Exchange) Cancel(
	ctx context.Context,
	coin string,
	oid int64,
	opts ...CancelOpt,
) (res *APIResponse[CancelOrderResponse], err error) {
	return e.BulkCancel(ctx, []CancelOrderRequest{
		{
			Coin:    coin,
			OrderID: oid,
		},
	}, opts...)
}

func (e *Exchange) BulkCancel(
	ctx context.Context,
	requests []CancelOrderRequest,
	opts ...CancelOpt,
) (res *APIResponse[CancelOrderResponse], err error) {
	cancels := make([]CancelOrderWire, 0, len(requests))
	for _, req := range requests {
		asset, ok := e.info.CoinToAsset(req.Coin)
		if !ok {
			return nil, fmt.Errorf("coin %s not found in info", req.Coin)
		}
		cancels = append(cancels, CancelOrderWire{
			Asset:   asset,
			OrderID: req.OrderID,
		})
	}

	action := CancelAction{
		Type:    "cancel",
		Cancels: cancels,
		Fast:    applyCancelOpts(opts).fast,
	}

	if err = e.executeAction(ctx, action, &res); err != nil {
		return
	}

	if res == nil || !res.Ok || res.Status == "err" {
		if res != nil && res.Err != "" {
			return res, fmt.Errorf("%s", res.Err)
		}
		return res, fmt.Errorf("cancel failed")
	}

	if err := res.Data.Statuses.FirstError(); err != nil {
		return res, err
	}

	return
}

type CancelOrderRequestByCloid struct {
	Coin  string
	Cloid string
}

func (e *Exchange) CancelByCloid(
	ctx context.Context,
	coin, cloid string,
	opts ...CancelOpt,
) (res *APIResponse[CancelOrderResponse], err error) {
	return e.BulkCancelByCloids(ctx, []CancelOrderRequestByCloid{
		{
			Coin:  coin,
			Cloid: cloid,
		},
	}, opts...)
}

func (e *Exchange) BulkCancelByCloids(
	ctx context.Context,
	requests []CancelOrderRequestByCloid,
	opts ...CancelOpt,
) (res *APIResponse[CancelOrderResponse], err error) {
	cancels := make([]CancelByCloidWire, len(requests))
	for i, req := range requests {
		normalizedCloid, err := normalizeCloid(&req.Cloid)
		if err != nil {
			return nil, fmt.Errorf("invalid cloid for cancel request %d: %w", i, err)
		}
		if normalizedCloid == nil {
			return nil, fmt.Errorf("cloid is required for cancel by cloid request %d", i)
		}
		asset, ok := e.info.CoinToAsset(req.Coin)
		if !ok {
			return nil, fmt.Errorf("coin %s not found in info", req.Coin)
		}

		cancels[i] = CancelByCloidWire{
			Asset:    asset,
			ClientID: *normalizedCloid,
		}
	}

	action := CancelByCloidAction{
		Type:    "cancelByCloid",
		Cancels: cancels,
		Fast:    applyCancelOpts(opts).fast,
	}

	if err = e.executeAction(ctx, action, &res); err != nil {
		return
	}

	if res == nil || !res.Ok || res.Status == "err" {
		if res != nil && res.Err != "" {
			return res, fmt.Errorf("%s", res.Err)
		}
		return res, fmt.Errorf("cancel failed")
	}

	if err := res.Data.Statuses.FirstError(); err != nil {
		return res, err
	}

	return
}

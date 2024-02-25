package parser

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/sahib/timeq"
	"github.com/sahib/timeq/bucket"
	"github.com/sahib/timeq/item"
	"github.com/sahib/timeq/vlog"
	"github.com/urfave/cli"
)

func optionsFromCtx(ctx *cli.Context) (timeq.Options, error) {
	opts := timeq.DefaultOptions()

	switch mode := ctx.GlobalString("sync-mode"); mode {
	case "full":
		opts.SyncMode = bucket.SyncFull
	case "data":
		opts.SyncMode = bucket.SyncData
	case "index":
		opts.SyncMode = bucket.SyncIndex
	case "none":
		opts.SyncMode = bucket.SyncNone
	default:
		return opts, fmt.Errorf("invalid sync mode: %s", mode)
	}

	bucketSize := ctx.GlobalDuration("bucket-size")
	if bucketSize <= 0 {
		return opts, fmt.Errorf("invalid bucket size: %v", bucketSize)
	}

	opts.BucketFunc = func(key item.Key) item.Key {
		return key / item.Key(bucketSize)
	}

	return opts, nil
}

func withQueue(fn func(ctx *cli.Context, q *timeq.Queue) error) cli.ActionFunc {
	return func(ctx *cli.Context) error {
		dir := ctx.GlobalString("dir")

		opts, err := optionsFromCtx(ctx)
		if err != nil {
			return fmt.Errorf("options: %w", err)
		}

		queue, err := timeq.Open(dir, opts)
		if err != nil {
			return fmt.Errorf("open: %w", err)
		}

		if err := fn(ctx, queue); err != nil {
			queue.Close()
			return err
		}

		if err := queue.Close(); err != nil {
			return fmt.Errorf("close: %w", err)
		}

		return nil
	}
}

// Run runs the timeq command line on `args` (args[0] should be os.Args[0])
func Run(args []string) error {
	app := cli.NewApp()
	app.Name = "timeq"
	app.Usage = "A persistent, time-based priority queue"
	app.Description = "This is a toy frontend to timeq. It's hellish inefficient, but nice to test behavior."
	app.Version = "0.0.1"

	cwd, err := os.Getwd()
	if err != nil {
		return err
	}

	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name:   "dir",
			Usage:  "Path to storage directory (defaults to curent working dir)",
			EnvVar: "TIMEQ_DIR",
			Value:  cwd,
		},
		cli.StringFlag{
			Name:   "sync-mode",
			Usage:  "What sync mode to use ('none', 'full', 'data', 'index')",
			EnvVar: "TIMEQ_SYNC_MODE",
			Value:  "full",
		},
		cli.DurationFlag{
			Name:   "bucket-size",
			Usage:  "The size of each bucket as time duration",
			EnvVar: "TIMEQ_BUCKET_SIZE",
			Value:  30 * time.Minute,
		},
	}

	app.Commands = []cli.Command{
		{
			Name:   "pop",
			Usage:  "Get one or several keys",
			Action: withQueue(handlePop),
			Flags: []cli.Flag{
				cli.IntFlag{
					Name:  "n,number",
					Usage: "Number of items to pop",
					Value: 1,
				},
			},
		}, {
			Name:   "push",
			Usage:  "Set one or a several key-value pairs",
			Action: withQueue(handlePush),
		}, {
			Name:    "len",
			Aliases: []string{"l"},
			Usage:   "Print the number of items in the queue",
			Action:  withQueue(handleLen),
		}, {
			Name:    "clear",
			Aliases: []string{"c"},
			Usage:   "Clear the queue until a certain point",
			Action:  withQueue(handleClear),
			Flags: []cli.Flag{
				cli.Int64Flag{
					Name:  "f,from",
					Usage: "Lowest key to delete key to delete (including)",
				},
				cli.Int64Flag{
					Name:  "t,to",
					Usage: "Highest key key to delete (including)",
				},
			},
		}, {
			Name:   "shovel",
			Usage:  "Move the data to another queue",
			Action: withQueue(handleShovel),
			Flags: []cli.Flag{
				cli.IntFlag{
					Name:     "d,dest",
					Usage:    "Directory of the destination queue",
					Required: true,
				},
			},
		}, {
			Name:  "fork",
			Usage: "Utilities for forks",
			Subcommands: []cli.Command{
				{
					Name:   "list",
					Usage:  "List all forks",
					Action: withQueue(handleForkList),
				}, {
					Name:   "create",
					Usage:  "Create a named fork",
					Action: withQueue(handleForkCreate),
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:     "n,name",
							Usage:    "Name of the fork",
							Required: true,
						},
					},
				}, {
					Name:   "remove",
					Usage:  "Remove a specific fork",
					Action: withQueue(handleForkRemove),
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:     "n,name",
							Usage:    "Name of the fork",
							Required: true,
						},
					},
				},
			},
		}, {
			Name:  "log",
			Usage: "Utilities for checking value logs",
			Subcommands: []cli.Command{
				{
					Name:   "dump",
					Usage:  "Print all values in the log",
					Action: handleLogDump,
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:     "p,path",
							Usage:    "Where the value log is",
							Required: true,
						},
					},
				},
			},
		},
	}

	return app.Run(args)
}

func handlePush(ctx *cli.Context, q *timeq.Queue) error {
	args := ctx.Args()
	items := make([]timeq.Item, 0, len(args))

	for _, arg := range args {
		split := strings.SplitN(arg, ":", 2)
		if len(split) < 2 {
			return fmt.Errorf("invalid tuple: %v", arg)
		}

		key, err := strconv.ParseInt(split[0], 10, 64)
		if err != nil {
			return err
		}

		items = append(items, timeq.Item{
			Key:  timeq.Key(key),
			Blob: []byte(split[1]),
		})
	}

	return q.Push(items)
}

func handlePop(ctx *cli.Context, q *timeq.Queue) error {
	n := ctx.Int("number")
	err := q.Pop(n, nil, func(items timeq.Items) error {
		for _, item := range items {
			fmt.Println(item)
		}

		return nil
	})

	if err != nil {
		return err
	}

	return nil
}

func handleLen(_ *cli.Context, q *timeq.Queue) error {
	fmt.Println(q.Len())
	return nil
}

func handleClear(ctx *cli.Context, q *timeq.Queue) error {
	if !ctx.IsSet("to") && !ctx.IsSet("from") {
		size := q.Len()
		if err := q.Clear(); err != nil {
			return err
		}

		fmt.Printf("deleted all %v items\n", size)
		return nil
	}

	from := ctx.Int64("from")
	if !ctx.IsSet("from") {
		from = math.MinInt64
	}

	to := ctx.Int64("to")
	if !ctx.IsSet("to") {
		to = math.MaxInt64
	}

	deleted, err := q.Delete(timeq.Key(from), timeq.Key(to))
	if err != nil {
		return err
	}

	fmt.Printf("deleted %v items\n", deleted)
	return nil
}

func handleShovel(ctx *cli.Context, srcQueue *timeq.Queue) error {
	dstDir := ctx.String("dest")

	dstOpts, err := optionsFromCtx(ctx)
	if err != nil {
		return err
	}

	dstQueue, err := timeq.Open(dstDir, dstOpts)
	if err != nil {
		return err
	}

	nShoveled, err := srcQueue.Shovel(dstQueue)
	if err != nil {
		return errors.Join(err, dstQueue.Close())
	}

	fmt.Printf("moved %d items\n", nShoveled)
	return dstQueue.Close()
}

func handleLogDump(ctx *cli.Context) error {
	log, err := vlog.Open(ctx.String("path"), true)
	if err != nil {
		return err
	}

	var loc = item.Location{Len: 1e9}
	for iter := log.At(loc, true); iter.Next(); {
		it := iter.Item()
		fmt.Printf("%v:%s\n", it.Key, it.Blob)
	}

	return log.Close()
}

func handleForkCreate(ctx *cli.Context, q *timeq.Queue) error {
	name := ctx.String("name")
	_, err := q.Fork(timeq.ForkName(name))
	return err
}

func handleForkList(_ *cli.Context, q *timeq.Queue) error {
	for _, fork := range q.Forks() {
		fmt.Println(fork)
	}

	return nil
}

func handleForkRemove(ctx *cli.Context, q *timeq.Queue) error {
	name := ctx.String("name")
	fork, err := q.Fork(timeq.ForkName(name))
	if err != nil {
		return err
	}

	return fork.Remove()
}

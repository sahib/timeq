package parser

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/sahib/timeq"
	"github.com/urfave/cli"
)

func optionsFromCtx(ctx *cli.Context) timeq.Options {
	// TODO: actually read options and decide which one make sense
	// bucket-size (convert to func)
	// max skew, sync options.
	return timeq.DefaultOptions()
}

func withQueue(fn func(ctx *cli.Context, q *timeq.Queue) error) cli.ActionFunc {
	return func(ctx *cli.Context) error {
		dir := ctx.GlobalString("dir")

		queue, err := timeq.Open(dir, optionsFromCtx(ctx))
		if err != nil {
			return err
		}

		if err := fn(ctx, queue); err != nil {
			queue.Close()
			return err
		}

		return queue.Close()
	}
}

// Run runs the timeq command line on `args` (args[0] should be os.Args[0])
func Run(args []string) error {
	app := cli.NewApp()
	app.Name = "timeq"
	app.Usage = "A persistent, time-based priority queue"
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
			Name:    "size",
			Aliases: []string{"s"},
			Usage:   "Print the number of items in the queue",
			Action:  withQueue(handleSize),
		}, {
			Name:    "clear",
			Aliases: []string{"c"},
			Usage:   "Clear the queue until a certain point",
			Action:  withQueue(handleClear),
			Flags: []cli.Flag{
				cli.IntFlag{
					Name:     "u,until",
					Usage:    "Until what key to delete",
					Required: true,
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
	items, err := q.Pop(n, make([]timeq.Item, 0, n))
	if err != nil {
		return err
	}

	for _, item := range items {
		fmt.Println(item)
	}

	return nil
}

func handleSize(ctx *cli.Context, q *timeq.Queue) error {
	fmt.Println(q.Size())
	return nil
}

func handleClear(ctx *cli.Context, q *timeq.Queue) error {
	deleted, err := q.DeleteLowerThan(timeq.Key(ctx.Int("until")))
	if err != nil {
		return err
	}

	fmt.Printf("deleted %v items\n", deleted)
	return nil
}

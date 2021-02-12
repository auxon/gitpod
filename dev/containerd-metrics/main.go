// Copyright (c) 2021 Gitpod GmbH. All rights reserved.
// Licensed under the GNU Affero General Public License (AGPL).
// See License-AGPL.txt in the project root for license information.

package main

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/containerd/containerd"
	apievents "github.com/containerd/containerd/api/events"
	"github.com/containerd/containerd/events"
	"github.com/containerd/typeurl"
	cli "github.com/urfave/cli/v2"

	"github.com/gitpod-io/gitpod/common-go/log"
)

func main() {
	app := &cli.App{
		Commands: []*cli.Command{
			{
				Name:  "export",
				Usage: "Starts exporting metrics",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:      "socket",
						TakesFile: true,
						Usage:     "path to the containerd socket",
						Required:  true,
					},
					&cli.BoolFlag{
						Name:  "verbose",
						Value: false,
					},
				},
				Action: func(c *cli.Context) error {
					log.Init("containerd-metrics", "", true, c.Bool("verbose"))
					return serveContainerdMetrics(c.String("socket"))
				},
			},
		},
	}
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

type Prep struct {
	T      time.Time
	Parent string
	Name   string
}
type Commit struct {
	Dur    time.Duration
	Parent string
	Name   string
	Key    string
}

type Image struct {
	Name      string
	TotalPrep time.Duration
	Layer     []Layer
}

type Layer struct {
	ID   string
	Prep time.Duration
}

var (
	prepByKey    = make(map[string]*Prep)
	commitByKey  = make(map[string]*Commit)
	commitByName = make(map[string]*Commit)
)

func serveContainerdMetrics(fn string) error {
	client, err := containerd.New(fn, containerd.WithDefaultNamespace("k8s.io"))
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	evts, errs := client.EventService().Subscribe(ctx)
	for {
		var e *events.Envelope
		select {
		case err := <-errs:
			return err
		case <-sigChan:
			return nil
		case e = <-evts:
		}

		evt, err := typeurl.UnmarshalAny(e.Event)
		if err != nil {
			log.WithError(err).Warn("skipping event")
			continue
		}

		switch evt := evt.(type) {
		case *apievents.SnapshotPrepare:
			prepByKey[evt.Key] = &Prep{
				T:      time.Now(),
				Parent: evt.Parent,
			}
			log.WithField("obj", *prepByKey[evt.Key]).Debug("prep")
		case *apievents.SnapshotCommit:
			p, ok := prepByKey[evt.Key]
			if !ok {
				log.WithField("key", evt.Key).WithField("name", evt.Name).Debug("found commit without prep")
				continue
			}
			c := &Commit{
				Dur:    time.Since(p.T),
				Name:   evt.Name,
				Parent: p.Parent,
				Key:    evt.Key,
			}
			commitByKey[evt.Key] = c
			commitByName[evt.Name] = c
			log.WithField("obj", *c).Debug("commit")
		case *apievents.SnapshotRemove:
			log.WithField("obj", evt).Info("snapshot remove")
		case *apievents.ImageDelete:
			log.WithField("obj", evt).Info("image delete")
		case *apievents.ContainerCreate:
			pushContainerMetrics(evt.ID, evt.Image)
		}
	}
}

func pushContainerMetrics(id, image string) {
	var img Image
	img.Name = image

	initialPrep, ok := prepByKey[id]
	if !ok {
		log.WithField("image", image).WithField("id", id).Debug("image witout prep")
		return
	}

	var c *Commit
	c, ok = commitByName[initialPrep.Parent]
	for ok {
		img.Layer = append(img.Layer, Layer{
			ID:   c.Name,
			Prep: c.Dur,
		})
		img.TotalPrep += c.Dur
		c, ok = commitByName[c.Parent]
	}

	for i, j := 0, len(img.Layer)-1; i < j; i, j = i+1, j-1 {
		img.Layer[i], img.Layer[j] = img.Layer[j], img.Layer[i]
	}

	segs := strings.Split(img.Name, "/")
	var instanceID string
	if len(segs) == 3 {
		instanceID = segs[2]
	}

	log.WithField("instanceId", instanceID).WithField("image", img).WithField("id", id).WithField("initialPrep", initialPrep.Parent).Info("image pulled")
}

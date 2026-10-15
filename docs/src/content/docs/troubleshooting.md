---
title: "Common problems"
---

## Plugin is absent from the CLI

The CLI's managed environment is separate from your project environment.

```bash
r2x --version
r2x python show
r2x list r2x-plexos
```

If missing, install with `r2x install r2x-plexos` and repeat the list command.
If configuration differs from this site, compare the installed release with [tagged source](/r2x-plexos/reference/versions/).

## Old constructor or return-value examples fail

Use `Parser.from_context(ctx).run()` as shown in [Python usage](/r2x-plexos/how-to/python/).
`run()` returns `PluginContext`, and lifecycle errors raise `PluginError`.
Examples using positional parser constructors or treating the returned context as `Result` do not match the current core lifecycle.

## XML or Model cannot be found

Pass a concrete XML file with `fpath` and confirm `model_name` exists in it.
If you supply an `xml_file` DataStore entry, check that entry first: it takes precedence.
For missing series, check referenced paths relative to the source data and the configured `timeseries_dir`.
Do not replace a missing profile with zeros to force the parser through.

## Template or export failure

Use a supported template key or an existing XML template file.
Check output-directory access and inspect the error's lifecycle stage.
Keep XML and generated CSVs together, then inspect memberships and simulation horizons in the exported model.

## Report a reproducible failure

Include package/CLI versions, the failing command, lifecycle error, and a minimal shareable input in a [GitHub issue](https://github.com/NatLabRockies/r2x-plexos/issues).
Remove confidential model data before attaching files.

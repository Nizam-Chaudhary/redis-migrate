# Publishing

This package publishes the `redis-op` CLI to npm. After publishing, users can run it with `npx`:

```sh
npx redis-op migrate -s redis://localhost:6379 -d redis://localhost:6380
npx redis-op export -s redis://localhost:6379 -o backup.ndjson
npx redis-op restore -d redis://localhost:6380 -i backup.ndjson
```

The legacy alias is also available:

```sh
npx redis-migrate -s redis://localhost:6379 -d redis://localhost:6380
```

## Package setup

The npm executable names are configured in `package.json`:

```json
"bin": {
  "redis-op": "./dist/cli.mjs",
  "redis-migrate": "./dist/cli.mjs"
}
```

The package uses `prepack` to build before `npm pack` or `npm publish`, and `files` ensures only the distributable build is included:

```json
"files": ["dist"],
"scripts": {
  "prepack": "npm run build"
}
```

## Before publishing

Run the validation commands:

```sh
npm run typecheck
npm pack --dry-run
```

Confirm the dry run only includes the expected publish files, primarily:

```text
dist/cli.mjs
package.json
```

You can also test the packed CLI locally:

```sh
npm pack
npx ./redis-op-1.0.0.tgz --help
```

Replace `1.0.0` with the current package version.

## Publish to npm

Log in to npm:

```sh
npm login
```

Publish the package:

```sh
npm publish --access public
```

For later releases, bump the version first:

```sh
npm version patch
npm publish --access public
```

Use `minor` or `major` instead of `patch` when the release requires it.

## Verify after publishing

Check the package metadata:

```sh
npm view redis-op name version bin
```

Run the published CLI:

```sh
npx redis-op --help
npx redis-op migrate --help
```

If `npx` appears to use an old version, clear the npm cache or specify the version explicitly:

```sh
npx redis-op@latest --help
```

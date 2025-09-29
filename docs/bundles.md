# Bundles

## Bundle API
Moat exposes an [OPA-compliant bundle API](https://www.openpolicyagent.org/docs/management-bundles#bundle-service-api) to
the OPA instance(s). Any number of OPA instances can be served by this endpoint. This pattern is aligned with OPA's preferred
method of single-source policy and metadata.

The API supports `ETag` caching using the standard `If-None-Match` header. OPA will supply the `Etag` assigned to a previously
requested bundle in the request for an updated bundle. If the bundle has not changed since the last request, the `ETag` will match
and the API will return a `HTTP:304` response. This pattern allows the OPA instances to poll the API fairly quickly without
putting significant load on the service.

## Bundle Generator
Bundles are regenerated in response to changes in policy docs (`rego` files) or changes to the resource or principal metadata.

The policy and metadata objects are monitored by a background process which regenerates the bundle after a short debouncing period.
This is to avoid recreating the bundle several times when multiple updates are delivered via the APIs.

Under normal operation, the new bundle should be available to the OPA instance(s) within 1-2 minutes of a change to policy or metadata. 

The bundle generator will produce bundle tarballs consisting of a `json` data object, `rego` policy documents and a `manifest`. The
`rego` policy files should be mounted at your chosen location, and this location configured using `bundle_generator.static_rego_file_path`.
The policy files will be provided in the bundle without modification, however the test files are removed to reduce the volume.
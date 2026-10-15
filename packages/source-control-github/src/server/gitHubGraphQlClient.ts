import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Redacted from "effect/Redacted";
import * as Stream from "effect/Stream";
import { HttpClient, HttpClientError } from "effect/http";
import * as Headers from "effect/http/Headers";
import { GraphQLClientError, GraphQLMiddleware, GraphQLProtocol } from "effect/graphql";
import { SourceControlProviderKind } from "@t3tools/contracts";
import * as SourceControlRateLimit from "@t3tools/source-control-core/server/SourceControlRateLimit";

import * as GitHubApi from "./GitHubApi.ts";
import * as GitHubCredentials from "./GitHubCredentials.ts";
import * as GitHubQuota from "./GitHubQuota.ts";

/**
 * The GitHub host and operation a generated GraphQL call is sent as. One client serves every host,
 * so each call names its own, the way `GitHubApi.graphql` takes them as arguments.
 */
export class GitHubGraphQlTarget extends Context.Service<
  GitHubGraphQlTarget,
  {
    readonly host: string;
    readonly operation: string;
    readonly allowReserve?: boolean | undefined;
  }
>()("@t3tools/source-control-github/server/gitHubGraphQlClient/GitHubGraphQlTarget") {}

/**
 * GitHub's auth and rate limits around a generated GraphQL call: the host's token, the quota
 * reserve and the host-wide pause before it is sent, and what GitHub's answer says about them after.
 * It fails with `GitHubApi`'s errors, which every GitHub caller already maps. GraphQL `errors` that
 * are not a rate limit are the client's: a batch settles them per pull request.
 *
 * Shares the credential, quota, pause and request slots with `GitHubApi`, so REST and GraphQL
 * reads spend and respect one view of each.
 */
export class GitHubGraphQlAuth extends GraphQLMiddleware.Service<
  GitHubGraphQlAuth,
  { requires: GitHubGraphQlTarget; error: GitHubApi.GitHubApiError }
>()("@t3tools/source-control-github/server/gitHubGraphQlClient/GitHubGraphQlAuth") {
  static readonly layer = Layer.effect(
    GitHubGraphQlAuth,
    Effect.gen(function* () {
      const api = yield* GitHubApi.GitHubApi;
      const credentials = yield* GitHubCredentials.GitHubCredentials;
      const quota = yield* GitHubQuota.GitHubQuota;
      const limits = yield* SourceControlRateLimit.SourceControlRateLimit;
      return GitHubGraphQlAuth.of({
        execute: ({ request, next }) =>
          Effect.gen(function* () {
            const target = yield* GitHubGraphQlTarget;
            const host = GitHubApi.normalizeHost(target.host);
            const context = { host, operation: target.operation };
            const allowReserve = target.allowReserve ?? (yield* SourceControlRateLimit.Interactive);
            const key = { provider: SourceControlProviderKind.make("github"), host };
            yield* Effect.annotateCurrentSpan({
              "github.host": host,
              "github.operation": target.operation,
              "github.kind": "graphql",
              "github.graphql.query_hash": GitHubApi.queryHash(request.query),
            });
            const { token, fingerprint } = yield* api.credential(host);
            const scope = yield* SourceControlRateLimit.CredentialScope;

            const send = Effect.gen(function* () {
              const lease = yield* quota
                .admit(host, "graphql", { allowReserve })
                .pipe(
                  Effect.andThen(
                    limits.check(key, allowReserve ? { allowPaused: true } : undefined),
                  ),
                );
              const answer = yield* next({
                ...request,
                headers: Headers.setAll(request.headers, {
                  ...GitHubApi.REQUEST_HEADERS,
                  authorization: `Bearer ${Redacted.value(token)}`,
                }),
              }).pipe(
                Effect.timeout(GitHubApi.DEFAULT_TIMEOUT),
                Effect.catchTags({
                  TimeoutError: (cause) =>
                    Effect.fail(new GitHubApi.GitHubApiRequestError({ ...context, cause })),
                }),
                Effect.catchIf(GraphQLClientError.isGraphQLClientError, (error) =>
                  refused(error, context, lease),
                ),
              );
              const headers = answer.headers ?? Headers.empty;
              yield* quota.observe(host, headers);
              yield* Effect.annotateCurrentSpan({
                ...(answer.status === undefined
                  ? {}
                  : { "http.response.status_code": answer.status }),
                ...GitHubApi.rateLimitAttributes(headers),
              });
              if (
                GitHubApi.isRateLimited({
                  status: answer.status ?? 200,
                  headers,
                  ...(answer.errors === undefined ? {} : { graphqlErrors: answer.errors }),
                })
              ) {
                return yield* rateLimited(headers, context, lease);
              }
              yield* limits.recordSuccess({ ...key, lease });
              return answer;
            });

            /** A refusal GitHub sent as an HTTP status, read the same way `GitHubApi` reads one. */
            const refused = (
              error: GraphQLClientError.GraphQLClientError,
              context: { readonly host: string; readonly operation: string },
              lease: number,
            ) =>
              Effect.gen(function* () {
                const reason = error.reason;
                if (reason._tag !== "TransportError" && reason._tag !== "ResponseError") {
                  return yield* Effect.fail(error);
                }
                const status = reason.status;
                const headers = reason.headers ?? Headers.empty;
                if (reason._tag === "TransportError" && status === undefined) {
                  return yield* new GitHubApi.GitHubApiRequestError({ ...context, cause: error });
                }
                yield* quota.observe(context.host, headers);
                // A secondary limit is a 403 that only its body names, which the transport read.
                const body =
                  reason._tag === "TransportError" &&
                  HttpClientError.isHttpClientError(reason.cause) &&
                  reason.cause.response !== undefined
                    ? yield* reason.cause.response.text.pipe(Effect.orElseSucceed(() => ""))
                    : "";
                if (
                  GitHubApi.isRateLimited({
                    status: status ?? 200,
                    headers,
                    body,
                    ...(reason._tag === "ResponseError" ? { graphqlErrors: reason.errors } : {}),
                  })
                ) {
                  return yield* rateLimited(headers, context, lease);
                }
                // The source may hold a newer token than the one that was refused.
                if (status === 401) {
                  yield* credentials.invalidate(context.host);
                  return yield* new GitHubApi.GitHubApiAuthenticationError(context);
                }
                if (status === 404) return yield* new GitHubApi.GitHubApiNotFoundError(context);
                return yield* new GitHubApi.GitHubApiResponseError({
                  ...context,
                  status: status ?? 200,
                });
              });

            const rateLimited = (
              headers: Headers.Headers,
              context: { readonly host: string; readonly operation: string },
              lease: number,
            ) =>
              Effect.gen(function* () {
                const retryAt = GitHubApi.retryAtFrom(headers, yield* Clock.currentTimeMillis);
                yield* limits.recordRateLimit({ ...key, lease, retryAt });
                yield* Effect.annotateCurrentSpan({
                  "github.rate_limited": true,
                  ...(retryAt === undefined ? {} : { "github.retry_at": retryAt }),
                });
                return yield* new GitHubApi.GitHubApiRateLimitError({
                  ...context,
                  ...(retryAt === undefined ? {} : { retryAt }),
                });
              });

            // The rate-limit scope follows the credential, so a pause on one account never blocks
            // another.
            return yield* api
              .withRequestSlot(send)
              .pipe(
                Effect.provideService(SourceControlRateLimit.CredentialScope, scope || fingerprint),
              );
          }).pipe(Effect.withSpan("GitHubGraphQlAuth.execute")),
        subscribe: () => Stream.die(new Error("GitHub GraphQL has no subscriptions.")),
      });
    }),
  );
}

/**
 * The HTTP transport, sending each request to its target host's endpoint. Resolvers batch by this
 * URL, so lookups for different hosts never share a request. `GitHubGraphQlAuth` requires the
 * target, so every call has one. The middleware's span is the request's: the client's own would add
 * `url.full`, which can carry SHAs and branch names, so it is off, as for REST.
 */
const layerProtocol = GraphQLProtocol.layerHttp({
  url: ({ context }) =>
    GitHubApi.gitHubApiUrls(Context.getUnsafe(context, GitHubGraphQlTarget).host).graphql,
}).pipe(
  Layer.provide(
    Layer.effect(
      HttpClient.HttpClient,
      Effect.gen(function* () {
        const client = yield* HttpClient.HttpClient;
        return HttpClient.transformResponse(
          client,
          Effect.provideService(HttpClient.TracerDisabledWhen, () => true),
        );
      }),
    ),
  ),
);

/**
 * What a client built over `.middleware(GitHubGraphQlAuth)` needs beyond the services `GitHubApi`
 * itself reads through.
 */
export const layer = Layer.merge(GitHubGraphQlAuth.layer, layerProtocol);

/**
 * What decides which lookups to one host may share a batched request: the credential and
 * rate-limit scope it will be sent under. Read from the context the lookup was made in. Resolvers
 * already keep hosts apart, since they batch by the transport's URL too.
 */
export const batchKey = (context: Context.Context<GitHubGraphQlTarget>) =>
  JSON.stringify([
    Context.getOrElse(context, GitHubApi.PinnedGitHubCredential, () => null)
      ?.credentialFingerprint ?? null,
    Context.getOrElse(context, SourceControlRateLimit.CredentialScope, () => ""),
  ]);

## Prepare the release

[Build both images](../build-images/) from one reviewed commit first. KUI does not currently publish official images. Replace every example namespace below with a registry namespace you control.

Use a unique tag for each reviewed commit, and configure immutable tags where the registry supports them. Keep the backend and frontend tag aligned.

## Publish to Docker Hub

Authenticate with Docker Hub, then tag and push both images:

```bash
docker login
KUI_REGISTRY_NAMESPACE="docker.io/your-dockerhub-user"
KUI_RELEASE_TAG="0.1.0-your-reviewed-commit"

docker tag kui-allinone:0.1.0 "$KUI_REGISTRY_NAMESPACE/kui-allinone:$KUI_RELEASE_TAG"
docker tag kui-frontend:0.1.0 "$KUI_REGISTRY_NAMESPACE/kui-frontend:$KUI_RELEASE_TAG"
docker push "$KUI_REGISTRY_NAMESPACE/kui-allinone:$KUI_RELEASE_TAG"
docker push "$KUI_REGISTRY_NAMESPACE/kui-frontend:$KUI_RELEASE_TAG"
```

Choose public or private repositories according to your deployment. Use the registry's credential flow; do not put passwords or tokens into committed scripts.

## Publish to GHCR

Use a credential with permission to publish packages and sign in interactively. Then use the same tag/push commands with a GHCR namespace:

```bash
docker login ghcr.io -u YOUR_GITHUB_USER
KUI_REGISTRY_NAMESPACE="ghcr.io/your-github-user"
```

Run the tag and push commands from the Docker Hub example with this namespace. Confirm package visibility and give the deployment identity pull access where needed.

## Publish to another registry

```bash
docker login registry.example.com
KUI_REGISTRY_NAMESPACE="registry.example.com/your-team"
```

Use the same release tag and image tagging sequence. Include the hostname and any required port in the namespace.

## Record digests and deploy

After publishing, inspect the registry reference:

```bash
docker buildx imagetools inspect "$KUI_REGISTRY_NAMESPACE/kui-allinone:$KUI_RELEASE_TAG"
docker buildx imagetools inspect "$KUI_REGISTRY_NAMESPACE/kui-frontend:$KUI_RELEASE_TAG"
```

Record the resulting digests in your release record and pin the deployment to them where possible. Retain the previous image pair and configuration for rollback.

For a distributed deployment, publish every backend image used by its Compose service graph, not only the all-in-one image. Follow [production deployment](../deployment/) for private service networks, signing keys, and probes.

Docker's [image tagging](https://docs.docker.com/reference/cli/docker/image/tag/) and [image pushing](https://docs.docker.com/reference/cli/docker/image/push/) references describe registry naming and push behavior.

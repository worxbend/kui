#!/usr/bin/env python3
"""Exercise shipped Compose policy, profile configuration, and nginx behavior.

Requires Docker/Compose. Starts only isolated, disposable proxy containers: no
Kafka, host-published ports, credentials, or running application stack is used.
For config-only checks (no containers), run:
    python3 scripts/test-deployment.py DemoTopologyPolicy QuickstartBindings ToolPins
"""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import textwrap
import time
import unittest
from urllib.parse import urlsplit


ROOT = Path(__file__).resolve().parent.parent
IMAGE = "nginxinc/nginx-unprivileged:1.27-alpine"
COMPOSE = [
    "docker", "compose", "--env-file", "/dev/null",
    "-f", str(ROOT / "deployment/quickstart/docker-compose.quickstart.yml"),
    "-f", str(ROOT / "deployment/frontend/docker-compose.frontend.yml"),
    "config", "--format", "json", "--no-env-resolution",
]


def run(*args, **kwargs):
    return subprocess.run(args, text=True, capture_output=True, check=True,
                          timeout=60, **kwargs)


def compose_environment(**overrides):
    # Do not inherit developer credentials or load a working-tree .env file.
    env = {key: os.environ[key] for key in ("PATH", "HOME", "TMPDIR")
           if key in os.environ}
    env.update(overrides)
    return env


def compose_config(*files):
    args = ["docker", "compose", "--env-file", "/dev/null"]
    for file in files:
        args.extend(("-f", str(ROOT / "deployment" / file)))
    return json.loads(run(*args, "config", "--format", "json",
                          "--no-env-resolution", env=compose_environment()).stdout)


def mounted_yaml(source):
    # Reuse Compose's YAML parser without a Python YAML dependency. Extension
    # fields preserve arbitrary YAML; interpolation and secret resolution stay
    # disabled. Nothing is started and the parsed document is never printed.
    document = "services: {}\nx-kui-config:\n" + textwrap.indent(source.read_text(), "  ")
    parsed = run("docker", "compose", "--env-file", "/dev/null",
                 "--project-directory", str(ROOT), "-p", "kui-config-check",
                 "-f", "-", "config", "--format", "json", "--no-interpolate",
                 "--no-env-resolution", input=document, env=compose_environment())
    return json.loads(parsed.stdout)["x-kui-config"]


class DemoTopologyPolicy(unittest.TestCase):
    TOPOLOGIES = {
        "compose/docker-compose.yml": {
            "kui-gateway", "kui-cluster", "kui-topic", "kui-message",
            "kui-consumer", "kui-schema", "kui-metrics", "kui-alerts",
            "kui-ksql", "kui-connect",
        },
        "compose/docker-compose.allinone.yml": {"kui"},
        "demo/docker-compose.demo.yml": {"kui"},
        "secured/docker-compose.secured.yml": {"kui"},
        "quickstart/docker-compose.quickstart.yml": {"kui"},
    }

    def test_only_demo_backends_opt_in_to_private_upstreams(self):
        for file, expected in self.TOPOLOGIES.items():
            with self.subTest(topology=file):
                services = compose_config(file)["services"]
                opted_in = {name for name, service in services.items()
                            if service.get("environment", {}).get(
                                "KUI_ALLOW_PRIVATE_UPSTREAMS") == "true"}
                self.assertEqual(opted_in, expected)
                for name in services.keys() - expected:
                    self.assertFalse("KUI_ALLOW_PRIVATE_UPSTREAMS" in
                                     services[name].get("environment", {}), name)

    def test_all_demo_backend_config_mounts_parse(self):
        for file, backends in self.TOPOLOGIES.items():
            services = compose_config(file)["services"]
            for name in sorted(backends):
                with self.subTest(topology=file, service=name):
                    service = services[name]
                    command = service["command"]
                    target = command[command.index("--config") + 1]
                    mounts = [volume for volume in service["volumes"]
                              if volume["target"] == target]
                    self.assertEqual(len(mounts), 1)
                    self.assertTrue(mounts[0]["read_only"])
                    source = Path(mounts[0]["source"])
                    self.assertTrue(source.is_relative_to(ROOT / "deployment"))
                    self.assertTrue(source.is_file(), f"missing config for {name}")
                    self.assertTrue(isinstance(mounted_yaml(source).get("kui"), dict))

    def test_overlays_preserve_backend_only_relaxation(self):
        combinations = (
            ("compose/docker-compose.yml", "compose/docker-compose.e2e.yml",
             "compose/docker-compose.observability.yml"),
            ("quickstart/docker-compose.quickstart.yml", "quickstart/docker-compose.auth.yml",
             "frontend/docker-compose.frontend.yml"),
        )
        for files in combinations:
            with self.subTest(base=files[0]):
                services = compose_config(*files)["services"]
                opted_in = {name for name, service in services.items()
                            if service.get("environment", {}).get(
                                "KUI_ALLOW_PRIVATE_UPSTREAMS") == "true"}
                self.assertEqual(opted_in, self.TOPOLOGIES[files[0]])

    def test_distributed_collectors_mount_authoritative_profiles(self):
        services = compose_config("compose/docker-compose.yml")["services"]
        for name in ("kui-alerts", "kui-metrics"):
            with self.subTest(service=name):
                service = services[name]
                command = service["command"]
                config_target = command[command.index("--config") + 1]
                mounts = [volume for volume in service["volumes"]
                          if volume["target"] == config_target]
                self.assertEqual(len(mounts), 1)
                self.assertTrue(mounts[0]["read_only"])
                source = Path(mounts[0]["source"])
                self.assertEqual(source, ROOT / "deployment/compose/kui-service.yaml")
                config = mounted_yaml(source)["kui"]
                url = config.get("clusterProfiles", {}).get("url")
                self.assertEqual(url, "http://kui-cluster:8080")
                target = urlsplit(url)
                self.assertIn(target.hostname, services)
                cluster = services[target.hostname]
                cluster_command = cluster["command"]
                cluster_target = cluster_command[cluster_command.index("--config") + 1]
                cluster_mount = next(volume for volume in cluster["volumes"]
                                     if volume["target"] == cluster_target)
                cluster_config = mounted_yaml(Path(cluster_mount["source"]))["kui"]
                self.assertEqual(target.port, cluster_config["server"]["port"])
                self.assertTrue(set(service["networks"]) & set(cluster["networks"]))
                # No environment/CLI override may quietly select a stale profile source.
                self.assertFalse(any(key.startswith("KUI_CLUSTERPROFILES_")
                                     for key in service.get("environment", {})))
                self.assertFalse(any(arg.startswith("--kui.clusterProfiles.")
                                     for arg in command))

    def test_production_examples_do_not_enable_demo_relaxation(self):
        for source in sorted((ROOT / "deployment/examples").glob("*.yaml")):
            with self.subTest(example=source.name):
                # Comments may explain the switch; no active example may enable it.
                active_lines = (line for line in source.read_text().splitlines()
                                if not line.lstrip().startswith("#"))
                self.assertFalse(any("KUI_ALLOW_PRIVATE_UPSTREAMS" in line
                                     for line in active_lines), source.name)


class ToolPins(unittest.TestCase):
    def test_storybook_server_is_pinned_and_resolved_locally(self):
        manifest = json.loads((ROOT / "frontend/package.json").read_text())
        pin = manifest["devDependencies"].get("http-server", "")
        self.assertRegex(pin, r"^\d+\.\d+\.\d+$")
        self.assertNotIn("npx", manifest["scripts"]["storybook:ci"])
        workflow = (ROOT / ".github/workflows/ci.yml").read_text()
        self.assertIn("pnpm exec http-server storybook-static", workflow)
        self.assertNotIn("npx --yes http-server", workflow)


class QuickstartBindings(unittest.TestCase):
    def bindings(self, **overrides):
        env = compose_environment(**overrides)
        config = json.loads(run(*COMPOSE, env=env).stdout)
        return [(name, port.get("host_ip"))
                for name, service in config["services"].items()
                for port in service.get("ports", [])]

    def test_default_bindings_are_loopback_only(self):
        bindings = self.bindings()
        self.assertEqual({name for name, _ in bindings},
                         {"kafka", "kafka-connect", "ksqldb-server", "schema-registry", "kui", "frontend"})
        self.assertTrue(all(address == "127.0.0.1" for _, address in bindings), bindings)

    def test_remote_exposure_requires_explicit_override(self):
        bindings = self.bindings(KUI_QUICKSTART_BIND_ADDRESS="0.0.0.0")
        self.assertTrue(bindings)
        self.assertTrue(all(address == "0.0.0.0" for _, address in bindings), bindings)


class FrontendProxy(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.containers = []
        cls.addClassCleanup(cls.cleanup_containers)
        cls.temp = tempfile.TemporaryDirectory(prefix="kui-proxy-")
        cls.addClassCleanup(cls.temp.cleanup)
        fixtures = Path(cls.temp.name)
        # Docker's nginx UID must be able to traverse the fixture directory.
        fixtures.chmod(0o755)
        (fixtures / "assets").mkdir()
        (fixtures / "assets/probe.js").write_text("export const probe = 'asset';\n")
        (fixtures / "icon.svg").write_text("<svg>probe</svg>\n")
        (fixtures / "index.html").write_text(
            (ROOT / "frontend/index.html").read_text())
        (fixtures / "upstream.conf").write_text(
            'server { listen 8080; large_client_header_buffers 4 16k; '
            'location / { return 200 "upstream:$request_uri"; } }\n')
        cls.prefixes = ("", "/kui", "/ops/kui")
        for prefix in cls.prefixes:
            container = run(
                "docker", "run", "--rm", "-d", "--network", "none", "--read-only",
                "--cap-drop", "ALL", "--security-opt", "no-new-privileges:true",
                "--tmpfs", "/tmp", "--tmpfs", "/etc/nginx/conf.d:mode=0777",
                "-e", f"KUI_BASE_PATH={prefix}",
                "-e", "KUI_GATEWAY_URL=http://127.0.0.1:8080",
                "-v", f"{fixtures}:/usr/share/nginx/html/ui:ro",
                "-v", f"{ROOT / 'deployment/frontend/entrypoint.sh'}:/configure.sh:ro",
                "--entrypoint", "sh", IMAGE, "-ec",
                "sh /configure.sh; cp /usr/share/nginx/html/ui/upstream.conf "
                "/etc/nginx/conf.d/upstream.conf; exec nginx -g 'daemon off;'",
            ).stdout.strip()
            cls.containers.append(container)
            # Bounded readiness check, not a fixed sleep or a background poller.
            deadline = time.monotonic() + 10
            while True:
                result = cls.fetch(container, "/healthz")
                if result.returncode == 0:
                    break
                if time.monotonic() >= deadline:
                    raise AssertionError(run("docker", "logs", container).stdout)
                time.sleep(0.05)

    @classmethod
    def cleanup_containers(cls):
        for container in cls.containers:
            subprocess.run(["docker", "rm", "-f", container], check=False,
                           capture_output=True, timeout=30)

    @staticmethod
    def fetch(container, path):
        return subprocess.run(
            ["docker", "exec", container, "wget", "-S", "-O", "-",
             f"http://127.0.0.1:8082{path}"],
            text=True, capture_output=True, timeout=10, check=False)

    def test_documents_assets_and_deep_links(self):
        for container, prefix in zip(self.containers, self.prefixes):
            for suffix in ("/ui/", "/ui/index.html", "/ui/clusters/demo/topics"):
                with self.subTest(prefix=prefix, suffix=suffix):
                    result = self.fetch(container, prefix + suffix)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn(f'<base href="{prefix}/ui/">', result.stdout)
                    self.assertIn("Cache-Control: no-store", result.stderr)
            for suffix, body in (("/ui/assets/probe.js", "export const probe"),
                                 ("/ui/icon.svg", "<svg>probe</svg>")):
                with self.subTest(prefix=prefix, suffix=suffix):
                    result = self.fetch(container, prefix + suffix)
                    self.assertEqual(result.returncode, 0, result.stderr)
                    self.assertIn(body, result.stdout)
            missing = self.fetch(container, prefix + "/ui/assets/missing.js")
            self.assertIn("404 Not Found", missing.stderr)
            self.assertNotIn("kui-bootstrap", missing.stdout)

    def test_api_preserves_prefix_and_query(self):
        for container, prefix in zip(self.containers, self.prefixes):
            with self.subTest(prefix=prefix):
                path = prefix + "/api/v1/health/ready?check=1"
                result = self.fetch(container, path)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(result.stdout, "upstream:" + path)

    def test_full_cursor_fits_proxy_request_line(self):
        # Synthetic wire-size boundary, not an authenticated application cursor.
        for container, prefix in zip(self.containers, self.prefixes):
            with self.subTest(prefix=prefix):
                path = (prefix + "/api/v1/clusters/demo/topics/orders/messages/stream"
                        "?cursor=" + "a" * 8192 + "&limit=100&direction=FORWARD")
                result = self.fetch(container, path)
                self.assertEqual(result.returncode, 0, result.stderr[:300])
                self.assertEqual(len(result.stdout), len("upstream:" + path))


if __name__ == "__main__":
    unittest.main(verbosity=2)

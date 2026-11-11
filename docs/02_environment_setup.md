# PStreamBench — Environment Setup

> **Status: PENDING.** Not yet written. This file needs to cover CloudLab
> provisioning, controller/WUI setup, and local Docker setup end to end.
> Real source material already exists and should be read + synthesized
> here rather than rewritten from scratch:
>
> - `PStreamBench_Cloud_setup/README.md` — CloudLab profile scope,
>   `install_all_tools.sh` / `install_<engine>_only.sh` / `pre_conditioning.sh`,
>   troubleshooting section (already has a "SSH Connection to Nodes Not
>   Working" section worth folding in).
> - `PStreamBench_controller/README.md` — `pip install -r requirements.txt`,
>   `python manage.py migrate`, `runserver`, CloudLab SSH access section.
> - `PStreamBench_wui/README.md` — `npm install` / `npm run serve`,
>   local vs. remote WUI setup.
> - `PStreamBench_local_docker/README.md` — the full Docker Compose path,
>   already written to reconstruction-guide depth for that one path
>   specifically (prerequisites, architecture table, per-engine run
>   sections, native-run appendix). This file's job is to fold that in as
>   one of several setup paths, not duplicate it.
> - This session's live 15-node CloudLab provisioning work (see
>   `docs/14_issue_log.md` INFRA-ENV-002/003) — the *actual* steps that
>   worked end-to-end on fresh nodes, including the pre-conditioning +
>   per-engine install split, which isn't written up as a standalone guide
>   anywhere yet.

Needs: machine requirements, a decision tree ("which setup path do I
want: local Docker proof-of-life vs. real CloudLab cluster"), and the
exact command sequence for each, cross-referenced to
`docs/13_rebuild_from_scratch.md`'s step numbering so the two files don't
drift apart.

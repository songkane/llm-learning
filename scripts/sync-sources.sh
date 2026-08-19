#!/usr/bin/env bash
#
# 检查「源码分析用到的上游代码库」清单，缺失的自动 clone 下来。
#
# 用法：
#   ./scripts/sync-sources.sh            # 检查全部，缺失的拉取
#   ./scripts/sync-sources.sh sglang     # 只检查指定的一个或多个
#   ./scripts/sync-sources.sh --list     # 只列出清单，不做任何操作
#
# 设计原则：**只负责「有没有」，不负责「是哪个版本」。**
#   - 已存在的仓库一律跳过，脚本不做 fetch / checkout / reset，
#     后续更新、切版本、对照历史都由你自己在目录里用正常 git 操作，
#     脚本不会干扰你的分支状态和本地批注。
#   - 缺失的用标准 git clone 拉取（不加 --depth / --single-branch），
#     保留完整历史、全部 tag 与远端分支，git 能力完全可用。
#   - clone 后 checkout 到清单登记的「文档基线」版本作为起点；
#     基线是 tag 时为 detached HEAD（正常现象，git checkout <分支> 即可回分支）。
#   - 代码实体存放在 iCloud 之外（默认 ~/Desktop/code/icloud-code），仓库内的
#     sources/ 只是指向它的软链接。原因：本仓库在 iCloud Drive 内，若把 2.6G
#     源码放进去，iCloud 会上传，且「优化存储」可能把 .git/objects 驱逐成占位
#     符，导致 git 卡顿或报错。可用 LLM_SOURCES_DIR 环境变量覆盖存放位置。
#   - sources 已在 .gitignore 中忽略，不会提交到本仓库。
#
set -euo pipefail

# name|git url|ref（tag 或分支），ref 需与各专题文档的「源码基线」一致
REPOS=(
  "vllm|https://github.com/vllm-project/vllm.git|main"
  "sglang|https://github.com/sgl-project/sglang.git|v0.5.16"
  "mooncake|https://github.com/kvcache-ai/Mooncake.git|v0.3.12.post1"
  "kubernetes|https://github.com/kubernetes/kubernetes.git|v1.36.3"
  "volcano|https://github.com/volcano-sh/volcano.git|v1.15.1"
  "kueue|https://github.com/kubernetes-sigs/kueue.git|v0.19.1"
)

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LINK="$ROOT/sources"
# 代码实体目录：默认放桌面 code/icloud-code，可用环境变量覆盖
DEST="${LLM_SOURCES_DIR:-$HOME/Desktop/code/icloud-code}"

list_repos() {
  printf '%-12s %-10s %s\n' "NAME" "BASELINE" "URL"
  local entry name url ref
  for entry in "${REPOS[@]}"; do
    IFS='|' read -r name url ref <<<"$entry"
    printf '%-12s %-10s %s\n' "$name" "$ref" "$url"
  done
}

if [[ "${1:-}" == "--list" || "${1:-}" == "-l" ]]; then
  list_repos
  exit 0
fi

mkdir -p "$DEST"

# 仓库内建立 sources -> $DEST 软链接，日常统一用 sources/ 路径访问
if [[ ! -e "$LINK" && ! -L "$LINK" ]]; then
  ln -s "$DEST" "$LINK"
  echo "已创建软链接：sources -> ${DEST}"
elif [[ -L "$LINK" && "$(readlink "$LINK")" != "$DEST" ]]; then
  ln -sfn "$DEST" "$LINK"
  echo "已更新软链接：sources -> ${DEST}"
fi

# 目标列表：无参数则全部
declare -a TARGETS=("$@")

# clone 完成后切到基线版本作为起点：ref 是远端分支则切分支，否则按 tag 处理
checkout_baseline() {
  local dir="$1" ref="$2"
  if git -C "$dir" show-ref --verify --quiet "refs/remotes/origin/${ref}"; then
    git -C "$dir" checkout --quiet "$ref"
  else
    git -C "$dir" checkout --quiet "refs/tags/${ref}"
  fi
}

cloned=0
skipped=0

check_one() {
  local name="$1" url="$2" ref="$3"
  local dir="$DEST/$name"

  if [[ -d "$dir/.git" ]]; then
    printf '[已存在] %-12s 当前 %s（跳过，交由你自己 git 管理）\n' \
      "$name" "$(git -C "$dir" describe --tags --always --dirty 2>/dev/null || echo '?')"
    skipped=$((skipped + 1))
    return
  fi

  # 有目录但没有 .git：上次 clone 中断的残留，清掉重来
  if [[ -e "$dir" ]]; then
    echo "[清理] ${name} 目录不完整（无 .git），重新拉取"
    rm -rf "$dir"
  fi

  echo "[拉取] ${name} <- ${url}（基线 ${ref}）"
  git clone "$url" "$dir"
  checkout_baseline "$dir" "$ref"
  printf '        HEAD %s / %s tags / %s 远端分支\n' \
    "$(git -C "$dir" describe --tags --always)" \
    "$(git -C "$dir" tag -l | wc -l | tr -d ' ')" \
    "$(git -C "$dir" branch -r | wc -l | tr -d ' ')"
  cloned=$((cloned + 1))
}

matched=0
for entry in "${REPOS[@]}"; do
  IFS='|' read -r name url ref <<<"$entry"
  if [[ ${#TARGETS[@]} -gt 0 ]]; then
    hit=0
    for t in "${TARGETS[@]}"; do
      [[ "$t" == "$name" ]] && hit=1
    done
    [[ $hit -eq 1 ]] || continue
  fi
  matched=$((matched + 1))
  check_one "$name" "$url" "$ref"
done

if [[ $matched -eq 0 ]]; then
  echo "没有匹配的代码库。可用条目：" >&2
  list_repos >&2
  exit 1
fi

echo
echo "检查完毕：新拉取 ${cloned} 个，已存在 ${skipped} 个。"
echo "代码实体：${DEST}"
echo "仓库内访问：${LINK}（软链接）"

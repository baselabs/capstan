# Compile-time configuration entry.
#
# capstan is a library — this config governs only its OWN dev/test builds. It is NOT shipped to Hex
# (see mix.exs `package.files`), so it never reaches a consumer's application. Runtime tunables (the
# dev MySQL substrate port, loaded from .env via Dotenvy) live in config/runtime.exs.
import Config

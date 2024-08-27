# TODO(https://github.com/bazelbuild/rules_rust/issues/2759): remove this file.

load(
    "@rules_foreign_cc//foreign_cc/private:framework.bzl",
    "get_foreign_cc_dep",
    "uniq_list_keep_order",
)
load("@rules_foreign_cc//foreign_cc/private:make_script.bzl", "pkgconfig_script")
load(
    "@rules_foreign_cc//foreign_cc/private/framework:helpers.bzl",
    "convert_shell_script",
    "shebang",
)

# Most content of the file is copied from @rules_foreign_cc//foreign_cc/private/framework.bzl

def extract_foreign_cc_deps(data):
    ext_build_dirs = []
    ext_files = []
    for dep in data:
        foreign_cc_dep = get_foreign_cc_dep(dep)
        if foreign_cc_dep:
            ext_build_dirs.extend([artifact.gen_dir for artifact in foreign_cc_dep.artifacts.to_list()])
            ext_files.append(dep[DefaultInfo].files)

    return uniq_list_keep_order(ext_build_dirs), ext_files

def prepare_pkg_config_script(ctx, ext_build_dirs):
    script_file = ctx.actions.declare_file("pkg_config_wrapper.sh")

    script_lines = [
        "export EXT_BUILD_ROOT=##pwd##",
        "export EXT_BUILD_DEPS=$$EXT_BUILD_ROOT$$/foreign_cc_deps",
        "##rm_rf## $$EXT_BUILD_DEPS$$",
        "##mkdirs## $$EXT_BUILD_DEPS$$",
    ] + _copy_deps_and_tools(ext_build_dirs) + pkgconfig_script(ext_build_dirs) + [
        "LINKFLAGS_FILE=",
        "LINKSEARCHPATHS_FILE=",
        "for arg in $$@$$; do",
        "  case $$arg$$ in",
        "    --link_flags=*)",
        "      LINKFLAGS_FILE=$${arg#--link_flags=}$$",
        "      ;;",
        "    --link_search_paths=*)",
        "      LINKSEARCHPATHS_FILE=$${arg#--link_search_paths=}$$",
        "      ;;",
        "  esac",
        "done",
        "\"$@\" && \\",
        "##replace_inplace## \"{}\" $$LINKFLAGS_FILE$$ $$LINKSEARCHPATHS_FILE$$".format(";".join(_replace_symlinked_deps(ext_build_dirs))),
    ]
    script_text = "\n".join([
        shebang(ctx),
        convert_shell_script(ctx, script_lines),
    ])

    ctx.actions.write(
        output = script_file,
        content = script_text,
        is_executable = True,
    )

    return script_file

def _copy_deps_and_tools(ext_build_dirs):
    lines = []
    for ext_dir in ext_build_dirs:
        lines.append("##symlink_to_dir## $$EXT_BUILD_ROOT$$/{} $$EXT_BUILD_DEPS$$ True".format(ext_dir.path))
    return lines

def _replace_symlinked_deps(ext_build_dirs):
    sed_script = []
    for ext_dir in ext_build_dirs:
        # TODO(https://github.com/bazelbuild/rules_rust/issues/2759): Remove this hack.
        # @rules_foreign_cc//private/framework.bzl creats a copy of the install
        # dir, which is used during foreign cc build actions. However, the built
        # target only present include dir and libraries files in CcInfo, so we
        # need to replace the copied install dir path to the original one.
        install_dir = ext_dir.path
        [left, _, right] = install_dir.rsplit("/", 2)
        install_dir = left + "/" + right
        sed_script.append("s|/foreign_cc_deps/{}/|/{}/|g".format(ext_dir.basename, install_dir))
    return sed_script

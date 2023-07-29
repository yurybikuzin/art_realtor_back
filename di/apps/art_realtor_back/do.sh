#!/usr/bin/env bash
set -e

source "$(realpath "$(dirname "${BASH_SOURCE[0]}")/../../sh/core/do.common.sh")"

src_rust_dir="$proj_dir/src/rust"
target="x86_64-unknown-linux-musl"
exe="target/$target/release/./$app"
dependencies_for_deploy=(
    # "$src_rust_dir/$app/./for-site.habit.json.gz"
    "$src_rust_dir/$exe" 
    "$src_rust_dir/$app/./.env"
    "$src_rust_dir/$app/./settings.toml"
    "$src_rust_dir/$app/./nohup.sh"
)

# pushd "$src_rust_dir/$app"
# for i in $(fd '\.(?:scss)$' assets); do
#     dependencies_for_deploy+=( "$src_rust_dir/$app/./${i%.scss}.css" )
# done
# for i in $(fd '\.(?:js)$' assets); do
#     dependencies_for_deploy+=( "$src_rust_dir/$app/./$i" )
# done
# popd

case $cmd in
    build )
        # pushd "$src_rust_dir/$app"
        # for style in $(fd '\.(?:scss)$' assets); do
        #     target_file="${style%.scss}.css"
        #     if [[ -f "$target_file" ]]; then
        #         chmod u+w "$target_file"
        #     fi
        #     ls -lAh "$style"
        #     grass "$style" > "$target_file"
        #     echo "$style => $target_file"
        #     chmod 445 "$target_file"
        # done
        # popd
        #
        # if [[ $BUILD != "NO" ]]; then
            pushd "$src_rust_dir" 
            x rustup target add "$target"
            x sudo apt install -y musl-tools
            x cargo build --release --target $target -p $app 
            x ls -lah $exe 
            popd 
        # fi
    ;;
    get-dependencies-for-deploy )
        echo "${dependencies_for_deploy[@]}"
    ;;
    deploy )
        [[ $dry_run ]] || set -e
        x $dry_run $src_rust_dir/$exe -w "$src_rust_dir/$app" -t 
        x $dry_run ssh "$host" "mkdir -p $proj/$kind/$app" 
        x $dry_run rsync -avz --relative "${dependencies_for_deploy[@]}" $host:$proj/$kind/$app/ 
    ;;
    after-deploy )
#         service_name="${app}_$kind"
#         if [[ $(ssh $host "ls /etc/systemd/system/${service_name}.service") ]]; then
#             if [[ $BUILD != "NO" ]]; then
#                 cmd="sudo systemctl restart ${app}_$kind && sudo systemctl enable ${app}_$kind"
#                 x $dry_run ssh $host "cd $proj/$kind/$app/ && $cmd"
#             fi

            # url="https://export.baza-winner.ru/art_realtor/for-site.habit.json.gz"
            # x $dry_run curl --silent --show-error --fail "$url" -o /dev/null

#             route=/$app
#             prefix=
#             if [[ $kind == 'prod' ]]; then
#                 prefix=""
#             else
#                 prefix="/$kind"
#             fi
#             url="$url$prefix$route/health"
#             cat << EOM
# == DID DEPLOY AND $cmd
# EOM
#         elif [[ -e $di_dir/apps/$app/systemd.service ]]; then
#             cat << EOM
# == AFTER DEPLOY NOTE:
#     run $di_dir/$kind/$app/systemd.sh
#     OR
#     Enter to '$host' via ssh and run 'cd $proj/$kind/$app && ./$app server' in tmux session
#     To leave ssh session use Enter-tilda-dot sequence (Enter ~ .)
# EOM
#         else
            # ls 
            cat << EOM
== DID DEPLOY
EOM
        # fi
    ;;
esac


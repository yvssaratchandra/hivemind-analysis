#!/bin/sh
# Run from any directory. Preserve the original deck and prepend the new slide.
set -eu
deck_dir=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
rsvg-convert --format=pdf --page-width=960pt --page-height=540pt \
    --width=960pt --height=540pt \
    --output="$deck_dir/opening-slide.pdf" "$deck_dir/opening-slide.svg"
pdfunite "$deck_dir/opening-slide.pdf" "$deck_dir/../../Pluraris Agora.pdf" \
    "$deck_dir/agora-with-fit-question.pdf"
rsvg-convert --format=pdf --page-width=960pt --page-height=540pt \
    --width=960pt --height=540pt \
    --output="$deck_dir/research-plan.pdf" "$deck_dir/research-plan.svg"
pdfunite "$deck_dir/agora-with-fit-question.pdf" "$deck_dir/research-plan.pdf" \
    "$deck_dir/agora-with-research-plan.pdf"

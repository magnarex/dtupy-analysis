python batch/make_submit.py -f scripts/dqm/pairing.py cosmic_240904_long cosmic_240904_long_log --f-index log --ncores 12 --cpus 12 --alias pair_log
python batch/make_submit.py -f scripts/dqm/pairing.py cosmic_240904_long cosmic_240904_long_std --f-index std --ncores 12 --cpus 12 --alias pair_std
python batch/make_submit.py -f scripts/dqm/pairing.py cosmic_240904_long cosmic_240904_long_pow2 --f-index pow2 --ncores 12 --cpus 12 --alias pair_pow2
python batch/make_submit.py -f scripts/dqm/pairing.py cosmic_240904_long cosmic_240904_long_none --f-index none --ncores 12 --cpus 12 --alias pair_none
python batch/make_submit.py -f scripts/dqm/pairing.py cosmic_240904_long cosmic_240904_long_ignore --f-index ignore --ncores 12 --cpus 12 --alias pair_ignore




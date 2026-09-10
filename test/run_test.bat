@REM Build and run all benchmark tests (v3 plugin syntax, scenario-for-scenario equivalent)
@REM Mapping: 旧 flag → 新段(见 docs/test-harness.md §6)
@REM   -T --task-base/-task-extra/-task-mean/-task-sigma → --task type/base/extra/mean/sigma
@REM   -U -t --submit-interval/-jitter/-mean-interval/-shards -P → --submit strategy/num/interval/jitter/mean-interval/shards/phases
@REM   -w -c -m → --pool workers/container/mode
@REM   -i -f -e → --metrics interval/format/wait-exit;旧工具自动文件名在此显式给出(file=),重复场景互相覆盖的行为与旧版一致
@REM   --cpuprofile --memprofile → --profile cpu=true mem=true
go build -o agilepool_test.exe .
agilepool_test.exe --pool workers=20000 --task type=fixed base=500 --submit strategy=immediate num=500000 --metrics interval=1 format=csv file=metrics_fixed_w20000_t500000_linkedlist.csv
agilepool_test.exe --pool workers=20000 --task type=uniform base=500 extra=50 --submit strategy=immediate num=500000 --metrics interval=1 format=csv file=metrics_uniform_w20000_t500000_linkedlist.csv
agilepool_test.exe --pool workers=20000 --task type=normal mean=500 sigma=50 --submit strategy=immediate num=500000 --metrics interval=1 format=csv file=metrics_normal_w20000_t500000_linkedlist.csv
agilepool_test.exe --pool workers=20000 --task type=fixed base=500 --submit strategy=linear num=20000 interval=5 jitter=3 --metrics interval=1 format=csv file=metrics_fixed_w20000_t20000_linkedlist.csv
agilepool_test.exe --pool workers=20000 --task type=fixed base=500 --submit strategy=poisson num=10000 mean-interval=10 --metrics interval=1 format=csv file=metrics_fixed_w20000_t10000_linkedlist.csv
agilepool_test.exe --pool workers=20000 --task type=fixed base=500 --submit strategy=phased phases="0,10,500;10,10,5000;20,10,500" shards=10 --metrics interval=1 format=csv file=metrics_fixed_w20000_t1000000_linkedlist.csv
agilepool_test.exe --pool workers=10000 container=linkedlist --task type=fixed base=500 --submit strategy=immediate num=200000 --metrics interval=1 format=csv file=metrics_fixed_w10000_t200000_linkedlist.csv
agilepool_test.exe --pool workers=10000 container=minheap --task type=fixed base=500 --submit strategy=immediate num=200000 --metrics interval=1 format=csv file=metrics_fixed_w10000_t200000_minheap.csv
agilepool_test.exe --pool workers=10000 container=slice --task type=fixed base=500 --submit strategy=immediate num=200000 --metrics interval=1 format=csv file=metrics_fixed_w10000_t200000_slice.csv
agilepool_test.exe --pool workers=10000 container=ringqueue --task type=fixed base=500 --submit strategy=immediate num=200000 --metrics interval=1 format=csv file=metrics_fixed_w10000_t200000_ringqueue.csv
agilepool_test.exe --pool workers=10000 mode=block --task type=fixed base=500 --submit strategy=immediate num=200000 --metrics interval=1 format=csv file=metrics_fixed_w10000_t200000_linkedlist.csv
agilepool_test.exe --pool workers=10000 mode=nonblock --task type=fixed base=500 --submit strategy=immediate num=200000 --metrics interval=1 format=csv file=metrics_fixed_w10000_t200000_linkedlist.csv
agilepool_test.exe --pool workers=100 --task type=fixed base=500 --submit strategy=immediate num=20000 --metrics interval=1 format=csv file=metrics_fixed_w100_t20000_linkedlist.csv
agilepool_test.exe --pool workers=500 --task type=fixed base=500 --submit strategy=immediate num=50000 --metrics interval=1 format=csv file=metrics_fixed_w500_t50000_linkedlist.csv
agilepool_test.exe --pool workers=2000 --task type=fixed base=500 --submit strategy=immediate num=100000 --metrics interval=1 format=csv file=metrics_fixed_w2000_t100000_linkedlist.csv
agilepool_test.exe --pool workers=10000 --task type=fixed base=500 --submit strategy=immediate num=200000 --metrics interval=1 format=csv file=metrics_fixed_w10000_t200000_linkedlist.csv
agilepool_test.exe --pool workers=10000 --task type=fixed base=100 --submit strategy=immediate num=200000 --metrics interval=1 format=csv file=metrics_fixed_w10000_t200000_linkedlist.csv
agilepool_test.exe --pool workers=10000 --task type=fixed base=500 --submit strategy=immediate num=200000 --metrics interval=1 format=csv file=metrics_fixed_w10000_t200000_linkedlist.csv
agilepool_test.exe --pool workers=10000 --task type=fixed base=2000 --submit strategy=immediate num=50000 --metrics interval=1 format=csv file=metrics_fixed_w10000_t50000_linkedlist.csv
agilepool_test.exe --profile cpu=true mem=true --pool workers=20000 --task type=fixed base=500 --submit strategy=immediate num=500000 --metrics interval=1 format=csv file=metrics_fixed_w20000_t500000_linkedlist.csv

@REM Complex orchestration tests
agilepool_test.exe --pool workers=10000 --task type=fixed base=500 --submit strategy=phased phases="0,30,1000" shards=5 --metrics interval=1 format=csv file=metrics_fixed_w10000_t1000000_linkedlist.csv
agilepool_test.exe --pool workers=15000 --task type=fixed base=500 --submit strategy=phased phases="0,3,200;3,3,5000;6,3,200;9,3,5000" shards=8 --metrics interval=1 format=csv file=metrics_fixed_w15000_t1000000_linkedlist.csv
agilepool_test.exe --pool workers=10000 mode=nonblock --task type=fixed base=500 --submit strategy=phased phases="0,10,2000;10,10,500" shards=5 --metrics interval=1 format=csv file=metrics_fixed_w10000_t1000000_linkedlist.csv
agilepool_test.exe --pool workers=20000 --task type=normal mean=500 sigma=100 --submit strategy=poisson num=15000 mean-interval=8 --metrics interval=1 format=csv file=metrics_normal_w20000_t15000_linkedlist.csv
agilepool_test.exe --pool workers=10000 mode=nonblock --task type=uniform base=500 extra=100 --submit strategy=linear num=10000 interval=10 jitter=5 --metrics interval=1 format=csv file=metrics_uniform_w10000_t10000_linkedlist.csv
agilepool_test.exe --pool workers=20000 --task type=fixed base=500 --submit strategy=constant num=15000 interval=8 --metrics interval=1 format=csv file=metrics_fixed_w20000_t15000_linkedlist.csv
agilepool_test.exe --pool workers=5000 --task type=fixed base=1 --submit strategy=immediate num=1000000 --metrics interval=1 format=csv file=metrics_fixed_w5000_t1000000_linkedlist.csv
agilepool_test.exe --pool workers=500 --task type=fixed base=3000 --submit strategy=immediate num=5000 --metrics interval=1 format=csv file=metrics_fixed_w500_t5000_linkedlist.csv
agilepool_test.exe --pool workers=10000 --task type=fixed base=500 --submit strategy=immediate num=200000 --metrics interval=1 format=csv file=metrics_fixed_w10000_t200000_linkedlist.csv wait-exit=3
agilepool_test.exe --pool workers=10000 container=minheap mode=nonblock --task type=uniform base=500 extra=100 --submit strategy=linear num=8000 interval=15 jitter=5 --metrics interval=1 format=json file=metrics_uniform_w10000_t8000_minheap.json wait-exit=5

python plot_csv.py

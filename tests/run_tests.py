import sys
import traceback
import pathlib
import importlib.util

REPO_ROOT = pathlib.Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

TEST_PATH = REPO_ROOT / "tests" / "test_orderflow_coverage.py"
spec = importlib.util.spec_from_file_location("test_orderflow_coverage", str(TEST_PATH))
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)

setup_module = getattr(mod, "setup_module")
test_warm_ws_book_healthy_thin_tape = getattr(mod, "test_warm_ws_book_healthy_thin_tape")
test_stream_not_warm = getattr(mod, "test_stream_not_warm")
test_record_gap_preserves_history = getattr(mod, "test_record_gap_preserves_history")


def run_test(fn):
    try:
        fn()
        print(f"OK: {fn.__name__}")
        return True
    except AssertionError as e:
        print(f"FAIL: {fn.__name__}: {e}")
        traceback.print_exc()
        return False
    except Exception as e:
        print(f"ERROR: {fn.__name__}: {e}")
        traceback.print_exc()
        return False


if __name__ == '__main__':
    setup_module(None)
    tests = [
        test_warm_ws_book_healthy_thin_tape,
        test_stream_not_warm,
        test_record_gap_preserves_history,
    ]

    MORE_PATH = REPO_ROOT / "tests" / "test_orderflow_coverage_more.py"
    spec2 = importlib.util.spec_from_file_location("test_orderflow_coverage_more", str(MORE_PATH))
    mod2 = importlib.util.module_from_spec(spec2)
    spec2.loader.exec_module(mod2)

    more_tests = [
        getattr(mod2, name)
        for name in dir(mod2)
        if name.startswith("test_")
    ]
    tests.extend(more_tests)

    CONF_PATH = REPO_ROOT / "tests" / "test_confirmation_CD.py"
    spec3 = importlib.util.spec_from_file_location("test_confirmation_CD", str(CONF_PATH))
    mod3 = importlib.util.module_from_spec(spec3)
    spec3.loader.exec_module(mod3)
    conf_tests = [getattr(mod3, name) for name in dir(mod3) if name.startswith("test_")]
    tests.extend(conf_tests)

    # Load cross-exchange unit tests
    CX_PATH = REPO_ROOT / "tests" / "test_crossexchange.py"
    if CX_PATH.exists():
        spec4 = importlib.util.spec_from_file_location("test_crossexchange", str(CX_PATH))
        mod4 = importlib.util.module_from_spec(spec4)
        spec4.loader.exec_module(mod4)
        cx_tests = [getattr(mod4, name) for name in dir(mod4) if name.startswith("test_")]
        tests.extend(cx_tests)

    ok = True
    for t in tests:
        ok = run_test(t) and ok
    if not ok:
        sys.exit(2)
    print("All tests passed")
    sys.exit(0)

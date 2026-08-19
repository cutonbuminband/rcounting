from rcounting.side_threads import dfa

threads = dfa.dfa_threads


def test_nrd_valid():
    nrd = dfa.dfa_threads["no repeating digits"].comment_type
    assert nrd.word_is_valid("987654")


def test_nrd_invalid():
    nrd = dfa.dfa_threads["no repeating digits"].comment_type
    assert not nrd.word_is_valid("987655")


def test_nrd_count():
    nrd = dfa.dfa_threads["no repeating digits"].comment_type
    assert nrd.comment_to_count("1000000") == 168570


def test_mrd_valid():
    mrd = dfa.dfa_threads["mostly repeating digits"].comment_type
    assert mrd.word_is_valid("9988776")


def test_mrd_ord_invalid():
    mrd = dfa.dfa_threads["mostly repeating digits"].comment_type
    assert not mrd.word_is_valid("998877")


def test_mrd_invalid():
    mrd = dfa.dfa_threads["mostly repeating digits"].comment_type
    assert not mrd.word_is_valid("9987")


def test_mrd_count():
    mrd = dfa.dfa_threads["mostly repeating digits"].comment_type
    assert mrd.comment_to_count("1000000") == 50059


def test_ncd_valid():
    ncd = dfa.dfa_threads["no consecutive digits"].comment_type
    assert ncd.word_is_valid("3795")


def test_ncd_invalid():
    ncd = dfa.dfa_threads["no consecutive digits"].comment_type
    assert not ncd.word_is_valid("37952")


def test_ncd_count():
    ncd = dfa.dfa_threads["no consecutive digits"].comment_type
    assert ncd.comment_to_count("1000000") == 103890


def test_nsd_valid():
    nsd = dfa.dfa_threads["no successive digits"].comment_type
    assert nsd.word_is_valid("39892")


def test_nsd_invalid():
    nsd = dfa.dfa_threads["no successive digits"].comment_type
    assert not nsd.word_is_valid("39982")


def test_nsd_count():
    nsd = dfa.dfa_threads["no successive digits"].comment_type
    assert nsd.comment_to_count("1000000") == 597870


def test_ocd_valid():
    ocd = dfa.dfa_threads["only consecutive digits"].comment_type
    assert ocd.word_is_valid("78675")


def test_ocd_invalid():
    ocd = dfa.dfa_threads["only consecutive digits"].comment_type
    assert not ocd.word_is_valid("78674")


def test_ocd_count():
    ocd = dfa.dfa_threads["only consecutive digits"].comment_type
    assert ocd.comment_to_count("1000000") == 33555


def test_mcd_valid():
    mcd = dfa.dfa_threads["mostly consecutive digits"].comment_type
    assert mcd.word_is_valid("78674")


def test_mcd_ocd_invalid():
    mcd = dfa.dfa_threads["mostly consecutive digits"].comment_type
    assert not mcd.word_is_valid("78675")


def test_mcd_invalid():
    mcd = dfa.dfa_threads["mostly consecutive digits"].comment_type
    assert not mcd.word_is_valid("786742")


def test_mcd_count():
    mcd = dfa.dfa_threads["mostly consecutive digits"].comment_type
    assert mcd.comment_to_count("1000000") == 179488


def test_ord_valid():
    ord = dfa.dfa_threads["only repeating digits"].comment_type
    assert ord.word_is_valid("5622566")


def test_ord_invalid():
    ord = dfa.dfa_threads["only repeating digits"].comment_type
    assert not ord.word_is_valid("5625566")


def test_ord_count():
    ord = dfa.dfa_threads["only repeating digits"].comment_type
    assert ord.comment_to_count("1000000") == 12843


def test_naot_valid():
    naot = dfa.dfa_threads["not any of those"].comment_type
    assert naot.word_is_valid("12345669")


def test_naot_nrd_invalid():
    naot = dfa.dfa_threads["not any of those"].comment_type
    assert not naot.word_is_valid("1234569")


def test_naot_ord_invalid():
    naot = dfa.dfa_threads["not any of those"].comment_type
    assert not naot.word_is_valid("123456691234569")


def test_naot_mrd_invalid():
    naot = dfa.dfa_threads["not any of those"].comment_type
    assert not naot.word_is_valid("12345669123456")


def test_naot_nsd_invalid():
    naot = dfa.dfa_threads["not any of those"].comment_type
    assert not naot.word_is_valid("91234569")


def test_naot_ncd_invalid():
    naot = dfa.dfa_threads["not any of those"].comment_type
    assert not naot.word_is_valid("13577531")


def test_naot_ocd_invalid():
    naot = dfa.dfa_threads["not any of those"].comment_type
    assert not naot.word_is_valid("1234566")


def test_naot_count():
    naot = dfa.dfa_threads["not any of those"].comment_type
    assert naot.comment_to_count("1000000") == 298692


def test_brd_valid():
    brd = dfa.dfa_threads["barely repeating digits"].comment_type
    assert brd.word_is_valid("11234")


def test_brd_nrd_invalid():
    brd = dfa.dfa_threads["barely repeating digits"].comment_type
    assert not brd.word_is_valid("1234")


def test_brd_invalid():
    brd = dfa.dfa_threads["barely repeating digits"].comment_type
    assert not brd.word_is_valid("123443")


def test_brd_count():
    brd = dfa.dfa_threads["barely repeating digits"].comment_type
    assert brd.comment_to_count("1000000") == 457731

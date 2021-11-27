flock = main

.PHONY : all clean test

all: clean $(flock)

test: clean
	raco test .

$(flock):
	raco scribble --htmls \
		++style css/extra.css \
		++style css/fancyverb.css \
		++extra js/accessibility.js \
		++xref-in setup/xref load-collections-xref \
		--redirect-main http://docs.racket-lang.org/ \
		$(flock).scrbl

clean:
	rm -rf $(flock)

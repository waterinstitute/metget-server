#!/usr/bin/perl
# (C) Copyright 2005- ECMWF.
#
# This software is licensed under the terms of the Apache Licence Version 2.0
# which can be obtained at http://www.apache.org/licenses/LICENSE-2.0.
#
# In applying this licence, ECMWF does not waive the privileges and immunities granted to it by
# virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
#
use strict;
use Data::Dumper;

my $name;
my %entries;

open(OUT,">grib_templates.h") or "die grib_templates.h: $!";
print OUT "/* This file is automatically generated by $0, do not edit */\n\n";

foreach $name ( @ARGV )
{
    # The old assumption that the file had a .grib extension
    # $name =~ /(\w+)\.grib/;
    #my $proc = $1;

    my $proc = $name;
    $proc =~ s/[^a-z0-9]/_/g;

	print "$name\n";

    open(IN,"<$name") or die "$name: $!";

	my $ccproc = $proc;
	$ccproc =~ s/\W/_/;

    print OUT << "EOF";

/*
    $name
*/

unsigned char _grib_template_${ccproc}\[\] = {

EOF

	my $len; my $data;
	my $size = 0;

	while (($len = read(IN,$data,8)))
	{
	     foreach my $x ( unpack('C*', $data) )
		 {
		 	printf OUT " 0x%02x,", $x;
			 $size++;
		 }
		 print OUT "\n";
	}


    print OUT << "EOF";

};


EOF

	$entries{"\t{\"$proc\", _grib_template_${ccproc}, $size, },"}++;

}


print OUT "\nstatic grib_templates templates[] = {\n";

foreach my $k ( sort keys %entries )
{
	print OUT "$k\n";
}

print OUT "};\n\n";

close(OUT) or "die grib_templates.h: $!";

print "Created grib_templates.h\n";

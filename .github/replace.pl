#!/usr/bin/perl
# Replace a specific string in a file with a placeholder value
# work with any string, including special characters

use strict;
use warnings;

# Check if both arguments are provided
if (@ARGV != 2) {
    die "Usage: perl replace_string.pl <string_to_replace> <file>\n";
}

my $string_to_replace = $ARGV[0];
my $file = $ARGV[1];

# Read the file content
open(my $fh, '<', $file) or die "Could not open file '$file' $!";
my $content = do { local $/; <$fh> };
close($fh);

# Replace the string
$content =~ s/\Q$string_to_replace\E/REDACTED/g;

# Write the updated content back to the file
open($fh, '>', $file) or die "Could not open file '$file' $!";
print $fh $content;
close($fh);

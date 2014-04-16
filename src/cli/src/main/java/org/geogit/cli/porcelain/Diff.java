/* Copyright (c) 2013 OpenPlans. All rights reserved.
 * This code is licensed under the BSD New License, available at the root
 * application directory.
 */

package org.geogit.cli.porcelain;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import jline.console.ConsoleReader;

import org.fusesource.jansi.Ansi;
import org.geogit.api.GeoGIT;
import org.geogit.api.plumbing.DiffBounds;
import org.geogit.api.plumbing.diff.DiffEntry;
import org.geogit.api.porcelain.DiffOp;
import org.geogit.cli.AbstractCommand;
import org.geogit.cli.AnsiDecorator;
import org.geogit.cli.CLICommand;
import org.geogit.cli.GeogitCLI;
import org.geogit.cli.annotation.ReadOnly;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.vividsolutions.jts.geom.Envelope;

/**
 * Shows changes between commits, commits and working tree, etc.
 * <p>
 * Usage:
 * <ul>
 * <li> {@code geogit diff [-- <path>...]}: compare working tree and index
 * <li> {@code geogit diff <commit> [-- <path>...]}: compare the working tree with the given commit
 * <li> {@code geogit diff --cached [-- <path>...]}: compare the index with the HEAD commit
 * <li> {@code geogit diff --cached <commit> [-- <path>...]}: compare the index with the given commit
 * <li> {@code geogit diff <commit1> <commit2> [-- <path>...]}: compare {@code commit1} with
 * {@code commit2}, where {@code commit1} is the eldest or left side of the diff.
 * </ul>
 * 
 * @see DiffOp
 */
@ReadOnly
@Parameters(commandNames = "diff", commandDescription = "Show changes between commits, commit and working tree, etc")
public class Diff extends AbstractCommand implements CLICommand {

    @Parameter(description = "[<commit> [<commit>]] [-- <path>...]", arity = 2)
    private List<String> refSpec = Lists.newArrayList();

    @Parameter(names = "--", hidden = true, variableArity = true)
    private List<String> paths = Lists.newArrayList();

    @Parameter(names = "--cached", description = "compares the specified tree (commit, branch, etc) and the staging area")
    private boolean cached;

    @Parameter(names = "--summary", description = "List only summary of changes")
    private boolean summary;

    @Parameter(names = "--nogeom", description = "Do not show detailed coordinate changes in geometries")
    private boolean nogeom;

    @Parameter(names = "--bounds", description = "Show only the bounds of the difference between the two trees")
    private boolean bounds;

    /**
     * Executes the diff command with the specified options.
     */
    @Override
    protected void runInternal(GeogitCLI cli) throws IOException {
        checkParameter(refSpec.size() <= 2, "Commit list is too long :%s", refSpec);
        checkParameter(!(nogeom && summary), "Only one printing mode allowed");

        GeoGIT geogit = cli.getGeogit();

        String oldVersion = resolveOldVersion();
        String newVersion = resolveNewVersion();

        if (bounds) {
            DiffBounds diff = geogit.command(DiffBounds.class).setOldVersion(oldVersion)
                    .setNewVersion(newVersion).setCompareIndex(cached);
            Envelope diffBounds = diff.call();
            BoundsDiffPrinter.print(geogit, cli.getConsole(), diffBounds);
            return;
        }

        DiffOp diff = geogit.command(DiffOp.class);
        diff.setOldVersion(oldVersion).setNewVersion(newVersion).setCompareIndex(cached);

        Iterator<DiffEntry> entries;
        if (paths.isEmpty()) {
            entries = diff.setProgressListener(cli.getProgressListener()).call();
        } else {
            entries = Iterators.emptyIterator();
            for (String path : paths) {
                Iterator<DiffEntry> moreEntries = diff.setFilter(path)
                        .setProgressListener(cli.getProgressListener()).call();
                entries = Iterators.concat(entries, moreEntries);
            }
        }

        if (!entries.hasNext()) {
            cli.getConsole().println("No differences found");
            return;
        }

        DiffPrinter printer;
        if (summary) {
            printer = new SummaryDiffPrinter();
        } else {
            printer = new FullDiffPrinter(nogeom, false);
        }

        DiffEntry entry;
        while (entries.hasNext()) {
            entry = entries.next();
            printer.print(geogit, cli.getConsole(), entry);
        }
    }

    private String resolveOldVersion() {
        return refSpec.size() > 0 ? refSpec.get(0) : null;
    }

    private String resolveNewVersion() {
        return refSpec.size() > 1 ? refSpec.get(1) : null;
    }

    private static final class BoundsDiffPrinter {

        public static void print(GeoGIT geogit, ConsoleReader console, Envelope envelope)
                throws IOException {

            Ansi ansi = AnsiDecorator.newAnsi(console.getTerminal().isAnsiSupported());

            if (envelope.isNull()) {
                ansi.a("No differences found.");
            } else {
                ansi.a(envelope.getMinX() + ", " + envelope.getMaxX() + ", " + envelope.getMinY()
                        + ", " + envelope.getMaxY());
            }

            console.println(ansi.toString());
        }

    }
}

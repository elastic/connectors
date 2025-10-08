#!/usr/bin/env python3
"""Quick test script for GitLab connector with real credentials."""

import asyncio
import os
import sys

from connectors.source import DEFAULT_CONFIGURATION, DataSourceConfiguration
from connectors.sources.gitlab import GitLabDataSource


async def test_gitlab_connector():
    """Test GitLab connector with real credentials."""

    # Get credentials from environment or prompt
    token = os.getenv("GITLAB_TOKEN")
    if not token:
        print("Please set GITLAB_TOKEN environment variable")
        print("Or run: export GITLAB_TOKEN='your-token-here'")
        sys.exit(1)

    # Optional: specify projects to test
    projects = os.getenv("GITLAB_PROJECTS", "*")

    print("=" * 80)
    print("GitLab Connector Test")
    print("=" * 80)
    print(f"Token: {token[:8]}..." if len(token) > 8 else "Token: [too short]")
    print(f"Projects filter: {projects}")
    print()

    # Create configuration properly like tests do
    config_dict = GitLabDataSource.get_default_configuration()
    config_dict["token"]["value"] = token
    config_dict["projects"]["value"] = projects

    config = DataSourceConfiguration(config=config_dict)

    # Initialize data source
    print("Initializing GitLab data source...")
    source = GitLabDataSource(configuration=config)

    try:
        # Test 1: Ping (authentication)
        print("\n" + "=" * 80)
        print("TEST 1: Ping / Authentication")
        print("=" * 80)
        await source.ping()
        print("✓ Authentication successful!")

        # Test 2: Validate configuration
        print("\n" + "=" * 80)
        print("TEST 2: Configuration Validation")
        print("=" * 80)
        await source.validate_config()
        print("✓ Configuration is valid!")

        # Test 3: Fetch documents
        print("\n" + "=" * 80)
        print("TEST 3: Fetching Documents")
        print("=" * 80)

        doc_counts = {
            "Project": 0,
            "Issue": 0,
            "Epic": 0,
            "Merge Request": 0,
            "Release": 0,
            "File": 0,
        }

        # Track one example document per type
        doc_examples = {}

        print("Fetching documents (showing first 20)...")
        count = 0
        async for doc, download_func in source.get_docs():
            count += 1
            doc_type = doc.get("type", "Unknown")
            doc_counts[doc_type] = doc_counts.get(doc_type, 0) + 1

            # Store first example of each type
            if doc_type not in doc_examples:
                doc_examples[doc_type] = doc

            # Print first 20 documents
            if count <= 20:
                doc_id = doc.get("_id")
                title = doc.get("title") or doc.get("name") or doc.get("file_name") or "N/A"
                print(f"  {count}. [{doc_type}] {doc_id}: {title[:60]}")

                # Show notes count if available
                if "notes" in doc:
                    total_notes = len(doc["notes"])
                    diff_notes = sum(1 for n in doc["notes"] if n.get("position"))
                    system_notes = sum(1 for n in doc["notes"] if n.get("system"))
                    print(f"      → {total_notes} notes (diff: {diff_notes}, system: {system_notes})")

                # If it's a file with download function, show we can download
                if download_func:
                    print(f"      → Has download function")

            # Stop after 100 for quick test
            if count >= 100:
                print(f"\n  ... stopping after {count} documents for quick test")
                break

        # Summary
        print("\n" + "=" * 80)
        print("SUMMARY")
        print("=" * 80)
        print(f"Total documents fetched: {count}")
        for doc_type, doc_count in sorted(doc_counts.items()):
            if doc_count > 0:
                print(f"  - {doc_type}: {doc_count}")

        # Print example documents
        print("\n" + "=" * 80)
        print("EXAMPLE DOCUMENTS (one per type)")
        print("=" * 80)

        import json

        for doc_type in sorted(doc_examples.keys()):
            print(f"\n{doc_type}:")
            print("-" * 80)
            doc = doc_examples[doc_type]
            # Pretty print the document structure
            print(json.dumps(doc, indent=2, default=str)[:1000000])  # Limit to 1000000 chars
            if len(json.dumps(doc, indent=2, default=str)) > 1000000:
                print("\n... (truncated)")
            print()

        # Test 4: Download a file (if we found any)
        if doc_counts.get("File", 0) > 0:
            print("\n" + "=" * 80)
            print("TEST 4: File Download")
            print("=" * 80)

            # Find a file to download
            async for doc, download_func in source.get_docs():
                if doc.get("type") == "File" and download_func:
                    file_name = doc.get("file_name", "unknown")
                    print(f"Testing download of: {file_name}")

                    # Try to download content
                    content_doc = await download_func(doit=True)
                    if content_doc:
                        has_attachment = "_attachment" in content_doc
                        has_body = "body" in content_doc
                        print(f"✓ Downloaded successfully!")
                        print(f"  - Has _attachment field: {has_attachment}")
                        print(f"  - Has body field: {has_body}")
                        if has_body:
                            body_preview = content_doc["body"][:150]
                            print(f"  - Content preview: {body_preview}...")
                        elif has_attachment:
                            # Show attachment size
                            import base64
                            attachment_data = content_doc.get("_attachment", "")
                            try:
                                decoded = base64.b64decode(attachment_data)
                                print(f"  - Attachment size: {len(decoded)} bytes")
                                # Try to decode as text for README
                                try:
                                    text_content = decoded.decode("utf-8")
                                    preview = text_content[:150].replace("\n", " ")
                                    print(f"  - Content preview: {preview}...")
                                except:
                                    print(f"  - Binary content (cannot preview)")
                            except:
                                print(f"  - Could not decode attachment")
                    else:
                        print("✗ Download returned None")
                    break

        print("\n" + "=" * 80)
        print("✓ ALL TESTS PASSED!")
        print("=" * 80)

    except Exception as e:
        print(f"\n✗ ERROR: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    finally:
        # Cleanup
        await source.close()


if __name__ == "__main__":
    asyncio.run(test_gitlab_connector())

"""Tests for SimpleMercator class."""

import pytest

from rudra.utils.mercator import SimpleMercator


class TestSimpleMercator:

    pom_xml_content = """
        <project>
            <dependencies>
                <dependency>
                    <groupId>grp1.id</groupId>
                    <artifactId>art1.id</artifactId>
                </dependency>
                <dependency>
                    <groupId>grp2.id</groupId>
                    <artifactId>art2.id</artifactId>
                </dependency>
                <dependency>
                    <groupId>grp3.id</groupId>
                    <artifactId>art3.id</artifactId>
                    <scope>test</scope>
                </dependency>
            </dependencies>
        </project>
    """

    def test_get_dependencies(self):
        client = SimpleMercator(self.pom_xml_content)
        deps = client.get_dependencies()
        assert len(deps) == 3
        artifact_ids = [d.artifact_id for d in deps]
        assert not {'art1.id', 'art2.id', 'art3.id'}.difference(set(artifact_ids))
        group_ids = [d.group_id for d in deps]
        assert not {'grp1.id', 'grp2.id', 'grp3.id'}.difference(set(group_ids))
        scopes = [d.scope for d in deps]
        assert not {'compile', 'test'}.difference(set(scopes))

    def test_get_dependencies_with_no_dependencies(self):
        client = SimpleMercator('<project></project>'.encode())
        deps = client.get_dependencies()
        assert len(deps) == 0

    def test_get_dependencies_with_no_content(self):
        with pytest.raises(ValueError, match='Empty Content .*'):
            SimpleMercator('')

    def test_find_data_corrupt_pom(self):
        content = """
        </project>
        </project>
        <dependencyManagement>
                <dependencies>
                    <dependency>
                        <groupId>grp1.id</groupId>
                        <artifactId>art1.id</artifactId>
                    </dependency>
                </dependencies>
        </dependencyManagement>
                <dependencies>
                    <dependency>
                        <groupId>grp1.id</groupId>
                        <artifactId>art1.id</artifactId>
                    </dependency>
                </dependencies>
        </project>
        """
        client = SimpleMercator(content)
        deps = client.get_dependencies()
        assert len(deps) == 1
        artifact_ids = [d.artifact_id for d in deps]
        assert 'art1.id' in artifact_ids

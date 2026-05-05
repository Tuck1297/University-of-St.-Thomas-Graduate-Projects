import { Container, Title, Text, Button, Stack, Box } from "@mantine/core";
import { Link } from "wouter";
import "../App.css";

function Home() {
  return (
    <Box className="home-container">
      <Container size="md">
        <Stack className="hero-section">
          <Title className="home-title" order={1}>
            Explore More
          </Title>

          <Stack gap="md" className="home-description">
            <Text>
              Through studies, attending tech-focused conferences, and
              professional experience as a front-end developer, I have
              recognized that those who work closely with data and retain domain
              knowledge are indispensable. This mindset drove this project,
              focusing on managing, transforming, and organizing data into a
              clear, flexible, and easy-to-manage data layer that can be
              utilized by any application.
            </Text>

            <Text>
              "Explore More" is a web application focused on collecting and
              sharing information about state, county, and national parks. The
              goal is to provide a source more reliable than Google Maps for
              those who simply want to explore their state.
            </Text>

            <Text>
              Finding information about parks often requires visiting multiple
              disparate sources like the MN DNR website, national forest pages,
              or county park sites. This project centralizes that information,
              solving the problem of sub-par collection from sources like Google
              and providing a single, reliable hub for outdoor enthusiasts.
            </Text>

            <Text>
              This project is a work in progress, so for the purposes of this
              course the frontend is minimal in nature and likely to have bugs.
              If you identify any bugs, please let me know via GitHub Issues or
              email. Additionally, if you have any feedback or suggestions for
              improvement, I would love to hear them!
            </Text>
          </Stack>

          <Link href="/map">
            <Button
              className="explore-button"
              variant="filled"
              color="blue"
              radius="xl"
              size="lg"
            >
              Explore the Map
            </Button>
          </Link>
        </Stack>
      </Container>
    </Box>
  );
}

export default Home;

{-# LANGUAGE DeriveGeneric   #-}
{-# LANGUAGE RecordWildCards #-}

module Main where

import           Control.Concurrent            (threadDelay)
import           Control.Monad                 (forever)
import           Control.Monad.IO.Class        (liftIO)
import           Data.Aeson                    (FromJSON (..), ToJSON (..))
import           GHC.Generics                  (Generic)
import           System.MQ.Component           (Env (..), ThreeChannels (..),
                                                load3Channels, runApp)
import           System.MQ.Component.Transport (pull)
import qualified System.MQ.Encoding.JSON       as JSON (pack, unpack)
import           System.MQ.Monad               (MQMonad)
import           System.MQ.Protocol            (Message (..), MessageLike (..),
                                                MessageType (..), Props (..),
                                                jsonEncoding)
import           Text.Printf                   (printf)

main :: IO ()
main = runApp "example_controlled-hs" exampleControlled

newtype ExampleSimpleConfig = ExampleSimpleConfig { config :: Int }
  deriving (Show, Generic)

instance ToJSON ExampleSimpleConfig

instance FromJSON ExampleSimpleConfig

instance MessageLike ExampleSimpleConfig where
  props = Props "example_simple" Config jsonEncoding
  pack = JSON.pack
  unpack = JSON.unpack

exampleControlled :: Env -> MQMonad ()
exampleControlled env@Env{..} = do
    ThreeChannels{..} <- load3Channels name
    forever $ do
        (tag, msg) <- pull fromController env

        liftIO $ printf "\nTag: %s\nMessage: %s" (show tag) (show (unpack . msgData $ msg :: Maybe ExampleSimpleConfig))
        liftIO $ threadDelay 1000000
